"""
Extra Putaway Sorting Guide — FastAPI backend
Run: uvicorn server:app --host 0.0.0.0 --port 8506 --reload
"""

import logging
import os
import sys
import time
import threading
import requests as http_requests
from pathlib import Path

from reservation import ReservationStore, ReservationPoller
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import create_engine, text

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("putaway")

# ---------------------------------------------------------------------------
# DB engine
# ---------------------------------------------------------------------------
_skills_config = Path(__file__).resolve().parent.parent.parent / "skills" / "config"
if str(_skills_config) not in sys.path:
    sys.path.insert(0, str(_skills_config))

try:
    from settings import engine
except ImportError:
    _user = os.environ.get("MYSQL_USER", "")
    _pass = os.environ.get("MYSQL_PASS", "")
    _host = os.environ.get("MYSQL_HOST", "rds.g3.yamibuy.net")
    engine = create_engine(
        f"mysql+pymysql://{_user}:{_pass}@{_host}/yamibuy_wh?charset=utf8mb4",
        pool_pre_ping=True,
    )

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
WAREHOUSES = {"001": "LA", "002": "NJ", "101": "ON"}
ZONES      = {"002": {"1": "NJFC", "2": "SFC"}}
ZONE_LABEL_TO_ID = {"002": {"NJFC": 1, "SFC": 2}}

# ---------------------------------------------------------------------------
# WMS API client (token cache + auto-refresh)
# ---------------------------------------------------------------------------
class WmsClient:
    def __init__(self):
        self.base_url = os.environ.get("WMS_BASE_URL", "").rstrip("/")
        self.user     = os.environ.get("WMS_USER", "")
        self.password = os.environ.get("WMS_PASS", "")
        self._token: str | None = None
        self._token_ts: float = 0
        self._lock = threading.Lock()
        self._token_ttl = 3600 * 8  # 8 hours

    def _login(self) -> str:
        r = http_requests.post(
            f"{self.base_url}/wms/common/loginWithoutWarehouseNum",
            json={"email": self.user, "password": self.password},
            timeout=10,
        )
        r.raise_for_status()
        body = r.json().get("body") or {}
        token = body.get("token")
        if not token:
            raise RuntimeError(f"WMS login failed: {r.text[:200]}")
        log.info("WMS token refreshed")
        return token

    def token(self) -> str:
        with self._lock:
            if not self._token or time.time() - self._token_ts > self._token_ttl:
                self._token = self._login()
                self._token_ts = time.time()
            return self._token

    def _get_latest_po(self, sku: str, wh: str) -> str | None:
        """Find the most recent inbound PO for this SKU in this warehouse."""
        sql = text("""
            SELECT inbound.reference_id AS po_number
            FROM yamibuy_wh.wh_inbound_batch batch
            INNER JOIN yamibuy_wh.wh_inbound inbound
                ON batch.inbound_number = inbound.inbound_number
            WHERE batch.item_number        = :sku
              AND inbound.warehouse_number = :wh
              AND batch.status            >= 0
            ORDER BY batch.in_dtm DESC
            LIMIT 1
        """)
        with engine.connect() as c:
            df = pd.read_sql(sql, c, params={"sku": sku, "wh": wh})
        if df.empty:
            return None
        return str(df.iloc[0]["po_number"])

    def get_empty_bins(self, wh: str, zone_label: str = "", limit: int = 5) -> list[dict]:
        """Fallback: pure empty bin list (no item info), used only if LR fails."""
        if not self.base_url or not self.user:
            return []
        try:
            resp = http_requests.post(
                f"{self.base_url}/wms/service/queryAvaiBinList",
                json={"warehouse_number": wh, "draw": 1, "startColumn": 0, "pageSize": 50},
                headers={"token": self.token()},
                timeout=12,
            )
            resp.raise_for_status()
            data = resp.json().get("body", {}) or {}
            rows = data.get("data") or []
            results = []
            for r in rows:
                loc_no = str(r.get("bin_sn") or "")
                if not loc_no:
                    continue
                if zone_label == "NJFC" and loc_no.upper().startswith("S"):
                    continue
                if zone_label == "SFC" and not loc_no.upper().startswith("S"):
                    continue
                actual_zone = "SFC" if (wh == "002" and loc_no.upper().startswith("S")) else ("NJFC" if wh == "002" else _zone_label(wh, None, loc_no))
                results.append({
                    "location_no":    loc_no,
                    "zone_id":        "",
                    "zone_label":     actual_zone,
                    "location_type":  "Bin",
                    "quantity":       0,
                    "allocated_qty":  0,
                    "expire_date":    None,
                    "days_to_expire": None,
                    "source":         "empty",
                })
                if len(results) >= limit:
                    break
            return results
        except Exception as e:
            log.warning(f"WMS get_empty_bins failed: {e}")
            return []

    def get_lr_recommendation(self, sku: str, wh: str, zone_label: str = "", limit: int = 5) -> list[dict]:
        """Get LR recommendations using latest historical PO."""
        if not self.base_url or not self.user:
            return []
        try:
            po = self._get_latest_po(sku, wh)
            if not po:
                log.info(f"No historical PO found for {sku} in {wh}, falling back to empty bins")
                return self.get_empty_bins(wh, zone_label, limit)

            resp = http_requests.post(
                f"{self.base_url}/wms/putaway/lr/queryLocationDetail",
                json={"item_number": sku, "warehouse_number": wh, "po_number": po},
                headers={"token": self.token()},
                timeout=15,
            )
            resp.raise_for_status()
            body = resp.json().get("body") or {}
            loc_list = body.get("locationList") or []

            results = []
            # Sort by ranking DESC, capacity ASC — best fit first
            sorted_locs = sorted(loc_list,
                                 key=lambda x: (-int(x.get("ranking") or 0),
                                                int(x.get("capacity") or 0)))
            for loc in sorted_locs:
                loc_no = str(loc.get("location_no") or "")
                if not loc_no:
                    continue
                # For 002/NJ: S-prefix = SFC, everything else = NJFC
                if wh == "002":
                    actual_zone = "SFC" if loc_no.upper().startswith("S") else "NJFC"
                else:
                    actual_zone = _zone_label(wh, None, loc_no)
                # zone filter
                if zone_label == "NJFC" and loc_no.upper().startswith("S"):
                    continue
                if zone_label == "SFC" and not loc_no.upper().startswith("S"):
                    continue
                results.append({
                    "location_no":    loc_no,
                    "zone_id":        "",
                    "zone_label":     actual_zone,
                    "location_type":  "Bin",
                    "quantity":       0,
                    "allocated_qty":  0,
                    "expire_date":    None,
                    "days_to_expire": None,
                    "source":         "lr",
                })
                if len(results) >= limit:
                    break

            if not results and sorted_locs:
                for loc in sorted_locs:
                    loc_no = str(loc.get("location_no") or "")
                    if not loc_no:
                        continue
                    if wh == "002":
                        actual_zone = "SFC" if loc_no.upper().startswith("S") else "NJFC"
                    else:
                        actual_zone = _zone_label(wh, None, loc_no)
                    results.append({
                        "location_no":    loc_no,
                        "zone_id":        "",
                        "zone_label":     actual_zone,
                        "location_type":  "Bin",
                        "quantity":       0,
                        "allocated_qty":  0,
                        "expire_date":    None,
                        "days_to_expire": None,
                        "source":         "lr",  # caller checks zone_label vs zone_filter for cross-zone
                    })
                    if len(results) >= limit:
                        break

            if not results:
                # LR returned nothing for this zone — fall back to empty bins
                return self.get_empty_bins(wh, zone_label, limit)
            return results
        except Exception as e:
            log.warning(f"WMS get_lr_recommendation failed: {e}")
            return self.get_empty_bins(wh, zone_label, limit)

wms_client = WmsClient()

# ---------------------------------------------------------------------------
# Reservation store + session poller
# ---------------------------------------------------------------------------
_db_path = str(Path(__file__).resolve().parent / "reservations.db")
reservation_store  = ReservationStore(db_path=_db_path)
reservation_poller = ReservationPoller(reservation_store, engine, idle_timeout=300)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _strip_i18n(val: str) -> str:
    if val and str(val).startswith("i18n##"):
        parts = str(val).split("##", 2)
        return parts[2] if len(parts) == 3 else val
    return val or ""


def _zone_label(wh: str, zone_id_raw, location_no: str = "") -> str:
    if wh not in ZONES:
        return ""
    if location_no and location_no.upper().startswith("S"):
        return "SFC"
    if pd.isna(zone_id_raw):
        return ""
    zid = str(int(zone_id_raw))
    return ZONES[wh].get(zid, "")


# ---------------------------------------------------------------------------
# DB queries
# ---------------------------------------------------------------------------
def lookup_item(raw: str) -> dict | None:
    sql = text("""
        SELECT u.item_number,
               g.goods_name,
               COALESCE(NULLIF(g.goods_ename,''), g.goods_name) AS goods_en_name
        FROM yamibuy_im.im_item_upc u
        JOIN yamibuy_im.im_item i        ON i.item_number = u.item_number
        JOIN yamibuy_master.xysc_goods g ON g.goods_id    = i.goods_id
        WHERE u.upc = :v OR u.item_number = :v
        LIMIT 1
    """)
    with engine.connect() as c:
        df = pd.read_sql(sql, c, params={"v": raw.strip()})
    if df.empty:
        return None
    return {
        "item_number":   str(df.iloc[0]["item_number"]),
        "goods_name":    _strip_i18n(str(df.iloc[0]["goods_name"])),
        "goods_en_name": _strip_i18n(str(df.iloc[0]["goods_en_name"])),
    }


def get_tote_assigned_location(tote: str, sku: str, wh: str) -> dict | None:
    sql = text("""
        SELECT p.target AS tote_id, p.to_location,
            CASE sl.location_type WHEN 3 THEN 'Stock' WHEN 4 THEN 'Bin'
                ELSE CAST(sl.location_type AS CHAR) END AS location_type,
            sl.zone_id, p.problem_qty, p.done_qty,
            CASE p.status WHEN 0 THEN 'Pending' WHEN 1 THEN 'Partial' END AS status,
            CONVERT_TZ(FROM_UNIXTIME(p.in_dtm),'UTC','America/Los_Angeles') AS created_at
        FROM yamibuy_wh.wh_pending_item p
        LEFT JOIN yamibuy_wh.wh_storage_location sl
            ON p.to_location = sl.location_no AND p.warehouse_number = sl.warehouse_number
        -- Validate: to_location must actually have inventory for this SKU
        INNER JOIN yamibuy_wh.wh_inventory_transaction inv
            ON p.to_location      = inv.location_no
            AND p.warehouse_number = inv.warehouse_number
            AND p.item_number      = inv.item_number
            AND inv.quantity       > 0
        WHERE p.warehouse_number = :wh AND p.item_number = :sku AND p.target = :tote
          AND p.status IN (0,1) AND p.to_location IS NOT NULL AND p.to_location != ''
        ORDER BY p.in_dtm DESC LIMIT 1
    """)
    with engine.connect() as c:
        df = pd.read_sql(sql, c, params={"wh": wh, "sku": sku, "tote": tote.strip().upper()})
    if df.empty:
        return None
    r = df.iloc[0]
    loc_no = str(r["to_location"] or "—")
    return {
        "location_no":   loc_no,
        "tote_id":       str(r["tote_id"]),
        "location_type": str(r["location_type"] or ""),
        "zone_id":       str(int(r["zone_id"])) if pd.notna(r["zone_id"]) else "",
        "zone_label":    _zone_label(wh, r["zone_id"], loc_no),
        "problem_qty":   int(r["problem_qty"]) if pd.notna(r["problem_qty"]) else 0,
        "done_qty":      int(r["done_qty"]) if pd.notna(r["done_qty"]) else 0,
        "status":        str(r["status"]),
        "created_at":    str(r["created_at"])[:16] if r["created_at"] else "",
        "source":        "pending",
    }


def get_current_locations(sku: str, wh: str, zone_id: int | None) -> list[dict]:
    if zone_id is not None and wh == "002":
        if zone_id == 2:
            zone_filter = "AND (li.location_no LIKE 'S%' OR sl.zone_id = 2)"
        else:
            zone_filter = "AND (li.location_no NOT LIKE 'S%' AND (sl.zone_id != 2 OR sl.zone_id IS NULL))"
    elif zone_id is not None:
        zone_filter = "AND sl.zone_id = :zone_id"
    else:
        zone_filter = ""

    sql = text(f"""
        SELECT li.location_no, sl.zone_id, li.quantity, li.allocated_qty,
            CASE WHEN sl.expire_dtm > 0 THEN DATE(FROM_UNIXTIME(sl.expire_dtm)) ELSE NULL END AS expire_date,
            CASE WHEN sl.expire_dtm > 0 THEN DATEDIFF(FROM_UNIXTIME(sl.expire_dtm), NOW()) ELSE NULL END AS days_to_expire
        FROM yamibuy_wh.wh_lot_inventory li
        JOIN yamibuy_wh.wh_storage_location sl
            ON li.location_no = sl.location_no AND li.warehouse_number = sl.warehouse_number
        WHERE li.item_number = :sku AND li.warehouse_number = :wh
          AND li.quantity > 0 AND sl.location_type = 4 {zone_filter}
        ORDER BY days_to_expire ASC, li.quantity ASC
    """)
    params: dict = {"wh": wh, "sku": sku}
    if zone_id is not None and wh != "002":
        params["zone_id"] = zone_id
    with engine.connect() as c:
        df = pd.read_sql(sql, c, params=params)
    rows = []
    for _, r in df.iterrows():
        loc_no = str(r["location_no"])
        days = int(r["days_to_expire"]) if pd.notna(r["days_to_expire"]) else None
        # Double-check zone in Python layer: S-prefix = SFC, skip if we want NJFC
        actual_zone = _zone_label(wh, r["zone_id"], loc_no)
        if zone_id is not None and wh == "002":
            wanted_zone = ZONES[wh].get(str(zone_id), "")
            if actual_zone != wanted_zone:
                continue  # skip rows that don't match selected zone
        rows.append({
            "location_no":    loc_no,
            "zone_id":        str(int(r["zone_id"])) if pd.notna(r["zone_id"]) else "",
            "zone_label":     _zone_label(wh, r["zone_id"], loc_no),
            "location_type":  "Bin",
            "quantity":       int(r["quantity"]),
            "allocated_qty":  int(r["allocated_qty"]) if pd.notna(r["allocated_qty"]) else 0,
            "expire_date":    str(r["expire_date"]) if r["expire_date"] else None,
            "days_to_expire": days,
            "source":         "current",
        })
    return rows


def get_ext_tote_summary(sku: str, wh: str) -> list[dict]:
    sql = text("""
        SELECT p.target AS tote_id, SUM(p.problem_qty) AS total_qty,
               MIN(p.to_location) AS sample_location,
               MIN(CONVERT_TZ(FROM_UNIXTIME(p.in_dtm),'UTC','America/Los_Angeles')) AS earliest_at
        FROM yamibuy_wh.wh_pending_item p
        WHERE p.warehouse_number = :wh AND p.item_number = :sku
          AND p.target LIKE 'EXT%' AND p.status IN (0,1)
        GROUP BY p.target ORDER BY total_qty DESC, earliest_at ASC
    """)
    with engine.connect() as c:
        df = pd.read_sql(sql, c, params={"wh": wh, "sku": sku})
    rows = []
    for _, r in df.iterrows():
        rows.append({
            "tote_id":   str(r["tote_id"]),
            "total_qty": int(r["total_qty"]) if pd.notna(r["total_qty"]) else 0,
            "earliest_at": str(r["earliest_at"])[:16] if r["earliest_at"] else "",
        })
    return rows


def get_size_id(sku: str, wh: str, tote_qty: int = 1) -> int | None:
    """
    Resolve the best-fit bin size_id for a SKU.
    Method 1: historical binding (most common size this SKU has been placed in).
    Method 2 (fallback): smallest size whose volume >= tote_qty × item volume.
    """
    # Method 1 — historical binding
    sql1 = text("""
        SELECT l.size_id, COUNT(1) AS cnt
        FROM yamibuy_wh.wh_storage_location_item sli
        INNER JOIN yamibuy_wh.wh_storage_location l
            ON sli.storage_location_id = l.rec_id
            AND l.warehouse_number     = sli.warehouse_number
        WHERE sli.item_number      = :sku
          AND sli.warehouse_number = :wh
          AND l.location_type      = 4
        GROUP BY l.size_id
        ORDER BY cnt DESC
        LIMIT 1
    """)
    with engine.connect() as c:
        df1 = pd.read_sql(sql1, c, params={"sku": sku, "wh": wh})
    if not df1.empty:
        return int(df1.iloc[0]["size_id"])

    # Method 2 — volume matching (total volume = qty × unit volume)
    qty = max(tote_qty, 1)
    sql2 = text("""
        SELECT s.size_id
        FROM yamibuy_wh.wh_location_size s
        CROSS JOIN (
            SELECT
                (CASE WHEN g.volume IS NULL OR g.volume = 0 THEN 50 ELSE g.volume END) AS item_volume,
                IFNULL(g.weight, 0) AS item_weight
            FROM yamibuy_im.im_item i
            INNER JOIN yamibuy_master.xysc_wearhouse_goods g ON i.goods_id = g.goods_id
            WHERE i.item_number = :sku
        ) item
        WHERE s.volume * s.fill_rate >= item.item_volume * :qty
          AND s.weight_capacity      >= item.item_weight * :qty
          AND s.description          != 'High Value'
        ORDER BY s.volume ASC
        LIMIT 1
    """)
    with engine.connect() as c:
        df2 = pd.read_sql(sql2, c, params={"sku": sku, "qty": qty})
    if not df2.empty:
        return int(df2.iloc[0]["size_id"])
    return None


def get_recommended_bins(sku: str, wh: str, zone_label: str,
                         tote_qty: int, occupancy: dict,
                         limit: int = 5,
                         continuous: bool = False) -> list[dict]:
    """
    Find available empty/mixed bins for a SKU using local SQL.
    Respects session occupancy (pre-occupied by other SKUs this session).
    """
    size_id = get_size_id(sku, wh, tote_qty)
    if size_id is None:
        return []

    # Zone filter
    if wh == "002":
        if zone_label == "NJFC":
            zone_sql = "AND l.location_no NOT LIKE 'S%'"
        elif zone_label == "SFC":
            zone_sql = "AND l.location_no LIKE 'S%'"
        else:
            zone_sql = ""
    else:
        zone_sql = ""

    # Continuous mode: sort by proximity to last reserved location
    if continuous:
        anchor = reservation_store.get_latest_reserved_location(wh, zone_label)
        order_sql = _build_continuous_order_sql(anchor)
        log.info(f"Continuous mode anchor={anchor}")
    else:
        order_sql = "has_bind DESC, capacity ASC"

    sql = text(f"""
        SELECT
            l.location_no,
            l.zone_id,
            l.item_count,
            l.max_sku,
            l.can_random,
            CASE WHEN sli.rec_id IS NULL THEN 0 ELSE 1 END AS has_bind,
            FLOOR(
                (s.volume * IFNULL(rate.fill_rate/100, 1) * s.fill_rate - l.g_volume)
                / item.volume
            ) AS capacity
        FROM yamibuy_wh.wh_storage_location l
        INNER JOIN yamibuy_wh.wh_location_size s ON s.size_id = l.size_id
        LEFT JOIN yamibuy_wh.wh_location_sku_fill_rate rate
            ON rate.location_type     = l.location_type
            AND rate.storage_type     = s.storage_type
            AND rate.max_sku          = l.max_sku
            AND rate.warehouse_number = l.warehouse_number
        LEFT JOIN yamibuy_wh.wh_storage_location_item sli
            ON l.rec_id            = sli.storage_location_id
            AND l.warehouse_number = sli.warehouse_number
            AND sli.item_number    = :sku
        CROSS JOIN (
            SELECT CASE WHEN g.volume IS NULL OR g.volume = 0 THEN 50 ELSE g.volume END AS volume
            FROM yamibuy_im.im_item i
            INNER JOIN yamibuy_master.xysc_wearhouse_goods g ON i.goods_id = g.goods_id
            WHERE i.item_number = :sku
        ) item
        WHERE l.location_type    = 4
          AND l.warehouse_number = :wh
          AND l.size_id          = :size_id
          AND l.business_flag    = 0
          AND LEFT(l.location_no, 2) != 'DC'
          AND (
              l.item_count = 0
              OR sli.rec_id IS NOT NULL
              OR l.can_random = 1
          )
          {zone_sql}
        HAVING capacity >= 1
        ORDER BY
            {order_sql}
        LIMIT 50
    """)
    with engine.connect() as c:
        df = pd.read_sql(sql, c, params={"sku": sku, "wh": wh, "size_id": size_id})

    results = []
    for _, row in df.iterrows():
        loc_no = str(row["location_no"])
        item_count = int(row["item_count"]) if pd.notna(row["item_count"]) else 0
        has_bind   = int(row["has_bind"])
        can_random = int(row["can_random"]) if pd.notna(row["can_random"]) else 0
        max_sku    = int(row["max_sku"]) if pd.notna(row["max_sku"]) else 1

        # Post-filter: for can_random mixed locations, check effective item count
        if item_count > 0 and has_bind == 0 and can_random:
            session_other = len(occupancy.get(loc_no, set()) - {sku})
            if max_sku <= item_count + session_other:
                continue  # no room

        actual_zone = "SFC" if (wh == "002" and loc_no.upper().startswith("S")) else (
                      "NJFC" if wh == "002" else "")
        results.append({
            "location_no":    loc_no,
            "zone_id":        str(int(row["zone_id"])) if pd.notna(row["zone_id"]) else "",
            "zone_label":     actual_zone,
            "location_type":  "Bin",
            "quantity":       0,
            "allocated_qty":  0,
            "expire_date":    None,
            "days_to_expire": None,
            "source":         "local_rec",
        })
        if len(results) >= limit:
            break
    return results


def _parse_location_prefix(location_no: str) -> dict:
    """
    Parse location_no into components for proximity sorting.
    Format: {zone}{aisle2}{bay1}{level1}{shelf1}  e.g. B00911
      zone  = location_no[0]       e.g. 'B'
      aisle = location_no[1:3]     e.g. '00'
      bay   = location_no[3]       e.g. '9'
    """
    loc = location_no.upper()
    return {
        "zone":  loc[0]    if len(loc) >= 1 else "",
        "aisle": loc[1:3]  if len(loc) >= 3 else "",
        "bay":   loc[3]    if len(loc) >= 4 else "",
        "zone_aisle":     loc[:3] if len(loc) >= 3 else loc,
        "zone_aisle_bay": loc[:4] if len(loc) >= 4 else loc,
    }


def _get_continuous_anchor(wh: str, zone_label: str) -> str | None:
    """
    Get the most recently reserved location_no in this session for proximity sorting.
    Returns None if no active reservations exist.
    """
    occupancy = reservation_store.get_session_occupancy(wh)
    if not occupancy:
        return None
    # Find most recent reservation from SQLite
    anchor = reservation_store.get_latest_reserved_location(wh, zone_label)
    return anchor


def _build_continuous_order_sql(anchor: str) -> str:
    """
    Build ORDER BY expressions that prioritize locations near the anchor.
    Priority: same zone+aisle+bay > same zone+aisle > same zone > any
    """
    if not anchor or len(anchor) < 4:
        return "has_bind DESC, capacity ASC"

    p = _parse_location_prefix(anchor)
    zone           = p["zone"]
    zone_aisle     = p["zone_aisle"]
    zone_aisle_bay = p["zone_aisle_bay"]

    return f"""
        CASE
            WHEN LEFT(l.location_no, 4) = '{zone_aisle_bay}' THEN 1
            WHEN LEFT(l.location_no, 3) = '{zone_aisle}'     THEN 2
            WHEN LEFT(l.location_no, 1) = '{zone}'           THEN 3
            ELSE 4
        END ASC,
        has_bind DESC,
        capacity ASC"""


def _build_reserved_row(location_no: str, wh: str, zone_label: str) -> dict:
    """Build a banner row dict from a SQLite-reserved location."""
    actual_zone = zone_label or (
        "SFC" if (wh == "002" and location_no.upper().startswith("S")) else
        "NJFC" if wh == "002" else ""
    )
    return {
        "location_no":    location_no,
        "zone_id":        "",
        "zone_label":     actual_zone,
        "location_type":  "Bin",
        "quantity":       0,
        "allocated_qty":  0,
        "expire_date":    None,
        "days_to_expire": None,
        "source":         "reserved",
    }


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------
@app.get("/api/lookup")
async def lookup(upc: str, wh: str = "002", zone: str = "", tote: str = "", continuous: str = "0"):
    if wh not in WAREHOUSES:
        return JSONResponse({"error": "invalid_warehouse"}, status_code=400)

    # Session reconciliation (runs once per session, zero cost otherwise)
    reservation_poller.on_lookup()
    is_continuous = continuous == "1"

    zone_id: int | None = None
    zone_label_display = ""
    if wh in ZONE_LABEL_TO_ID and zone in ZONE_LABEL_TO_ID[wh]:
        zone_id = ZONE_LABEL_TO_ID[wh][zone]
        zone_label_display = zone

    try:
        item = lookup_item(upc)
        if item is None:
            return JSONResponse({"error": "item_not_found"})
        sku = item["item_number"]
        current   = get_current_locations(sku, wh, zone_id)
        ext_totes = get_ext_tote_summary(sku, wh)

        # tote_qty for size recommendation (qty of this SKU in current tote)
        tote_upper = tote.strip().upper() if tote else ""
        tote_qty = 1
        if tote_upper:
            for t in ext_totes:
                if t["tote_id"] == tote_upper:
                    tote_qty = max(t["total_qty"], 1)
                    break

        if not tote:
            return JSONResponse({
                "item_number": sku, "goods_en_name": item["goods_en_name"],
                "goods_name": item["goods_name"], "warehouse": wh,
                "wh_name": WAREHOUSES[wh], "zone_filter": zone_label_display,
                "mode": "lookup", "current": current, "ext_totes": ext_totes,
            })

        wrong_zone = False
        not_in_tote = False
        banner_source = "none"
        banner_rows: list[dict] = []
        assigned = get_tote_assigned_location(tote_upper, sku, wh)

        # Check whether this SKU exists at all in the scanned tote (any status)
        tote_has_sku = any(t["tote_id"] == tote_upper for t in ext_totes)

        if assigned:
            if zone_id is not None and assigned["zone_label"] != zone_label_display:
                # Assigned to a different zone — try selected zone first
                wrong_zone = True
                if current:
                    # Selected zone has stock — recommend it
                    banner_source = "current_zone"
                    banner_rows = current
                else:
                    # Selected zone empty — fall back to the assigned location (different zone)
                    banner_source = "pending"
                    banner_rows = [assigned]
            else:
                banner_source = "pending"
                banner_rows = [assigned]
        elif not tote_has_sku:
            # SKU not found in this tote at all — warn and show lookup
            not_in_tote = True
            banner_source = "current_zone"
            if current:
                banner_rows = current
            elif zone_id is not None:
                banner_rows = get_current_locations(sku, wh, None)
                banner_source = "current_other_zone"
            else:
                # No stock anywhere — step 4: reuse SQLite reservation
                reused = reservation_store.get_location_for_sku(sku, wh, zone_label_display)
                if reused:
                    banner_rows = [_build_reserved_row(reused, wh, zone_label_display)]
                    banner_source = "reserved"
                else:
                    # Step 5: local SQL recommendation
                    occupancy = reservation_store.get_session_occupancy(wh)
                    banner_rows = get_recommended_bins(sku, wh, zone_label_display, tote_qty, occupancy, continuous=is_continuous)
                    banner_source = "local_rec" if banner_rows else "none"
                    # Step 6: WMS empty bin fallback
                    if not banner_rows:
                        banner_rows = wms_client.get_empty_bins(wh, zone_label_display)
                        banner_source = "empty_bin" if banner_rows else "none"
        else:
            # In tote but no assigned location — try current stock first
            if current:
                banner_source = "current_zone"
                banner_rows = current
            elif zone_id is not None:
                banner_source = "current_other_zone"
                banner_rows = get_current_locations(sku, wh, None)
            if not banner_rows:
                # Step 4: reuse SQLite reservation for same SKU
                reused = reservation_store.get_location_for_sku(sku, wh, zone_label_display)
                if reused:
                    banner_rows = [_build_reserved_row(reused, wh, zone_label_display)]
                    banner_source = "reserved"
                else:
                    # Step 5: local SQL recommendation
                    occupancy = reservation_store.get_session_occupancy(wh)
                    banner_rows = get_recommended_bins(sku, wh, zone_label_display, tote_qty, occupancy, continuous=is_continuous)
                    banner_source = "local_rec" if banner_rows else "none"
                    # Step 6: WMS empty bin fallback
                    if not banner_rows:
                        banner_rows = wms_client.get_empty_bins(wh, zone_label_display)
                        banner_source = "empty_bin" if banner_rows else "none"

        return JSONResponse({
            "item_number": sku, "goods_en_name": item["goods_en_name"],
            "goods_name": item["goods_name"], "warehouse": wh,
            "wh_name": WAREHOUSES[wh], "zone_filter": zone_label_display,
            "mode": "putaway", "tote": tote_upper,
            "banner_source": banner_source, "banner_rows": banner_rows,
            "current": current, "ext_totes": ext_totes,
            "wrong_zone": wrong_zone, "not_in_tote": not_in_tote, "assigned": assigned,
        })

    except Exception as e:
        log.exception("lookup error")
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/debug/location")
async def debug_location(loc: str, wh: str = "002"):
    sql = text("SELECT location_no, warehouse_number, zone_id, location_type FROM yamibuy_wh.wh_storage_location WHERE location_no = :loc AND warehouse_number = :wh")
    with engine.connect() as c:
        df = pd.read_sql(sql, c, params={"loc": loc, "wh": wh})
    if df.empty:
        return JSONResponse({"error": "not_found"})
    row = df.iloc[0].to_dict()
    row["zone_label"] = _zone_label(wh, row["zone_id"], str(row.get("location_no", "")))
    return JSONResponse(row)


@app.post("/api/reserve")
async def reserve_location(body: dict):
    tote_id     = str(body.get("tote_id", "")).strip().upper()
    sku         = str(body.get("sku", "")).strip()
    location_no = str(body.get("location_no", "")).strip()
    wh          = str(body.get("wh", "")).strip()
    zone_label  = str(body.get("zone_label", "")).strip()
    if not tote_id.startswith("EXT"):
        return JSONResponse({"error": "tote_id must start with EXT"}, status_code=400)
    if wh not in WAREHOUSES:
        return JSONResponse({"error": "invalid_warehouse"}, status_code=400)
    if not sku or not location_no:
        return JSONResponse({"error": "sku and location_no required"}, status_code=400)
    result = reservation_store.reserve(tote_id, sku, location_no, wh, zone_label)
    log.info(f"Reserved: tote={tote_id} sku={sku} loc={location_no} wh={wh}")
    return JSONResponse({"ok": True, **result})


@app.delete("/api/reserve/{tote_id}")
async def release_reservation(tote_id: str):
    n = reservation_store.release_by_tote(tote_id.strip().upper())
    return JSONResponse({"ok": True, "tote_id": tote_id.upper(), "rows_released": n})


@app.delete("/api/reserve/row/{row_id}")
async def delete_reservation_row(row_id: int):
    """Delete a single reservation row by id."""
    import time
    now = time.time()
    import sqlite3 as _sqlite3
    conn = _sqlite3.connect(reservation_store._db_path)
    cur = conn.execute("UPDATE reservations SET released_at=? WHERE id=? AND released_at IS NULL", (now, row_id))
    conn.commit()
    conn.close()
    return JSONResponse({"ok": cur.rowcount > 0, "row_id": row_id})


@app.get("/api/reservations")
async def list_reservations(wh: str = "002"):
    return JSONResponse({"reservations": reservation_store.list_active(wh)})


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(HTML_PAGE)


HTML_PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no">
<meta name="apple-mobile-web-app-capable" content="yes">
<title>Putaway Guide</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>🏷</text></svg>">
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent;}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#f0f2f5;height:100dvh;display:flex;flex-direction:column;overflow:hidden;}
/* Header */
#header{display:flex;align-items:center;gap:8px;background:#fff;padding:10px 14px;border-bottom:1px solid #e0e0e0;flex-shrink:0;}
#header .title{font-size:1rem;font-weight:700;white-space:nowrap;flex-shrink:0;}
.hdr-select{font-size:.88rem;padding:6px 26px 6px 10px;border:1.5px solid #d0d0d0;border-radius:8px;background:#fafafa;outline:none;cursor:pointer;-webkit-appearance:none;appearance:none;background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='6' viewBox='0 0 10 6'%3E%3Cpath d='M1 1l4 4 4-4' stroke='%23999' stroke-width='1.5' fill='none' stroke-linecap='round'/%3E%3C/svg%3E");background-repeat:no-repeat;background-position:right 8px center;}
#wh-select{min-width:108px;}
#zone-select{min-width:80px;display:none;}
#zone-select.visible{display:block;}
#zone-select.njfc{border-color:#34a853;color:#2e7d32;background-color:#f3fbf4;}
#zone-select.sfc{border-color:#4285f4;color:#1a73e8;background-color:#f0f4ff;}
#continuous-toggle{font-size:.78rem;font-weight:700;padding:5px 10px;border-radius:8px;border:1.5px solid #ddd;background:#fafafa;color:#999;cursor:pointer;white-space:nowrap;flex-shrink:0;}
#continuous-toggle.on{border-color:#34a853;background:#f0faf3;color:#2e7d32;}
/* Tote bar */
#tote-bar{background:#fff;padding:8px 14px;border-bottom:1px solid #e0e0e0;flex-shrink:0;display:flex;align-items:center;gap:8px;}
.tote-label{font-size:.7rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:#999;white-space:nowrap;flex-shrink:0;}
#tote-input{flex:1;font-size:.9rem;padding:6px 10px;border:1.5px solid #ddd;border-radius:8px;background:#fafafa;outline:none;}
#tote-input:focus{border-color:#f9ab00;background:#fff;}
#tote-input.confirmed{border-color:#f9ab00;background:#fffbe6;color:#333;font-weight:600;}
#tote-clear{background:none;border:none;font-size:1rem;color:#bbb;cursor:pointer;padding:4px;line-height:1;flex-shrink:0;display:none;}
#tote-clear.visible{display:block;}
.tote-mode-badge{font-size:.68rem;font-weight:700;padding:2px 8px;border-radius:20px;white-space:nowrap;flex-shrink:0;}
.badge-putaway{background:#fff3cd;color:#856404;}
.badge-lookup{background:#e8f0fe;color:#1a73e8;}
/* Search */
#search-area{background:#fff;padding:10px 14px;border-bottom:1px solid #e0e0e0;flex-shrink:0;}
.search-wrap{display:flex;align-items:center;gap:8px;background:#f5f5f5;border:1.5px solid #ddd;border-radius:10px;padding:0 12px;transition:border-color .15s;}
.search-wrap:focus-within{border-color:#1a73e8;background:#fff;}
.search-icon{font-size:1rem;color:#aaa;flex-shrink:0;}
#upc-input{flex:1;border:none;background:transparent;font-size:1rem;padding:10px 0;outline:none;color:#222;}
#clear-btn{background:none;border:none;font-size:1.1rem;color:#bbb;cursor:pointer;padding:4px;line-height:1;display:none;}
#clear-btn.visible{display:block;}
/* Result */
#result-area{flex:1;overflow-y:auto;padding:12px 14px;-webkit-overflow-scrolling:touch;}
.product-pill{display:flex;align-items:center;gap:10px;background:#fff;border-radius:10px;padding:10px 14px;margin-bottom:12px;box-shadow:0 1px 3px rgba(0,0,0,.08);}
.sku-badge{background:#e8f0fe;color:#1a73e8;font-size:.72rem;font-weight:700;padding:3px 8px;border-radius:20px;white-space:nowrap;flex-shrink:0;}
.product-name{font-size:.85rem;color:#333;line-height:1.4;}
.notice-bar{background:#fff3e0;border:1.5px solid #fb8c00;border-radius:8px;padding:9px 14px;font-size:.8rem;color:#e65100;margin-bottom:10px;}
/* Destination banner */
.dest-banner{border-radius:12px;padding:18px 18px 14px;margin-bottom:12px;display:flex;align-items:center;justify-content:space-between;gap:12px;}
.dest-banner.assigned{background:#fffbe6;border:2px solid #f9ab00;}
.dest-banner.current{background:#f0faf3;border:2px solid #34a853;}
.dest-banner.warn{background:#fde8e8;border:2px solid #e57373;}
.dest-banner.zone-njfc{background:#f0faf3;border:2px solid #34a853;}
.dest-banner.zone-sfc{background:#f0faf3;border:2px solid #34a853;}
.dest-banner.zone-cross{background:#fffbe6;border:2px solid #f9ab00;}
.dest-left{display:flex;flex-direction:column;gap:4px;}
.dest-label{font-size:.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:#888;}
.dest-loc{font-family:"SF Mono","Roboto Mono",monospace;font-size:2rem;font-weight:800;letter-spacing:1px;line-height:1.1;color:#222;}
.dest-meta{font-size:.75rem;color:#888;margin-top:2px;}
.dest-zone-wrap{display:flex;flex-direction:column;align-items:flex-end;gap:6px;flex-shrink:0;}
.dest-zone-chip{font-size:1.05rem;font-weight:800;padding:6px 14px;border-radius:10px;white-space:nowrap;letter-spacing:.5px;}
.dest-zone-chip.njfc{background:#e8f5e9;color:#2e7d32;border:1.5px solid #66bb6a;}
.dest-zone-chip.sfc{background:#e8f0fe;color:#1a73e8;border:1.5px solid #4285f4;}
.dest-zone-chip.def{background:#f5f5f5;color:#757575;border:1.5px solid #ccc;}
/* Print button — in dest-zone-wrap, below zone chip */
.print-btn{background:#1a73e8;border:none;border-radius:8px;padding:7px 14px;font-size:.85rem;font-weight:600;color:#fff;cursor:pointer;white-space:nowrap;}
.print-btn:active{background:#1558b0;}
/* Tote strip */
.tote-strip{display:flex;align-items:flex-start;gap:10px;background:#fff;border-radius:10px;padding:10px 14px;margin-bottom:12px;box-shadow:0 1px 3px rgba(0,0,0,.08);}
.tote-strip-label{font-size:.7rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:#999;white-space:nowrap;padding-top:3px;flex-shrink:0;}
.tote-chips{display:flex;flex-wrap:wrap;gap:6px;}
.tote-chip{display:inline-flex;align-items:center;gap:5px;background:#fff8e1;border:1.5px solid #f9ab00;border-radius:20px;padding:3px 10px;cursor:pointer;transition:background .15s;}
.tote-chip:hover{background:#fff3cd;}
.tote-chip.active{background:#f9ab00;border-color:#e65100;}
.tote-chip.active .tote-id,.tote-chip.active .tote-qty{color:#fff;}
.tote-id{font-family:"SF Mono","Roboto Mono",monospace;font-size:.78rem;font-weight:700;color:#333;}
.tote-qty{font-size:.72rem;color:#f9ab00;font-weight:700;}
/* Section cards */
.section-card{background:#fff;border-radius:10px;margin-bottom:12px;box-shadow:0 1px 3px rgba(0,0,0,.08);overflow:hidden;}
.section-header{display:flex;align-items:center;gap:8px;padding:10px 14px;border-bottom:1px solid #f0f0f0;}
.section-dot{width:10px;height:10px;border-radius:50%;flex-shrink:0;}
.dot-assigned{background:#f9ab00;}.dot-current{background:#34a853;}.dot-empty{background:#bbb;}
.section-title{font-size:.82rem;font-weight:700;text-transform:uppercase;letter-spacing:.05em;color:#444;}
.section-count{margin-left:auto;font-size:.72rem;color:#999;background:#f5f5f5;padding:2px 8px;border-radius:20px;}
.loc-list{padding:4px 0;}
.loc-row{display:flex;align-items:center;padding:9px 14px;border-bottom:1px solid #fafafa;gap:10px;flex-wrap:wrap;}
.loc-row:last-child{border-bottom:none;}
.loc-no{font-family:"SF Mono","Roboto Mono",monospace;font-size:.95rem;font-weight:600;color:#222;flex-shrink:0;min-width:90px;}
.zone-chip{font-size:.68rem;font-weight:700;padding:2px 8px;border-radius:20px;white-space:nowrap;flex-shrink:0;}
.zone-njfc{background:#e8f5e9;color:#2e7d32;}.zone-sfc{background:#e8f0fe;color:#1a73e8;}.zone-def{background:#f5f5f5;color:#757575;}
.loc-meta{font-size:.75rem;color:#888;flex:1;text-align:right;white-space:nowrap;}
.exp-soon{color:#e65100;font-weight:600;}
.unavailable{display:flex;align-items:center;justify-content:center;padding:18px 14px;color:#999;font-size:.85rem;gap:6px;}
.spinner-wrap{text-align:center;padding:40px;color:#aaa;font-size:.9rem;}
.spinner{width:28px;height:28px;border:3px solid #e0e0e0;border-top-color:#1a73e8;border-radius:50%;animation:spin .7s linear infinite;margin:0 auto 10px;}
@keyframes spin{to{transform:rotate(360deg);}}
.error-msg{background:#fde8e8;color:#c62828;border-radius:8px;padding:12px 14px;font-size:.85rem;}
.hint{display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%;gap:10px;color:#ccc;}
.hint .icon{font-size:3rem;}.hint p{font-size:.85rem;text-align:center;}
/* Print */
@media print{
  @page{margin:0;size:1.5in 0.5in landscape;}
  body *{visibility:hidden !important;}
  #print-label,#print-label *{visibility:visible !important;}
  #print-label{position:fixed;top:0;left:0;width:1.5in;height:0.5in;display:flex !important;align-items:center;justify-content:center;background:#fff;}
}
/* Reservation panel */
#reservation-panel{background:#fff;border-top:1px solid #e0e0e0;flex-shrink:0;}
#reservation-toggle{display:flex;align-items:center;gap:8px;padding:10px 14px;cursor:pointer;user-select:none;}
#reservation-toggle:hover{background:#f5f5f5;}
#reservation-title{font-size:.78rem;font-weight:700;color:#555;text-transform:uppercase;letter-spacing:.05em;flex:1;}
#reservation-count-badge{font-size:.68rem;font-weight:700;background:#f9ab00;color:#fff;padding:2px 7px;border-radius:20px;display:none;}
#reservation-chevron{font-size:.7rem;color:#aaa;transition:transform .2s;}
#reservation-chevron.open{transform:rotate(90deg);}
#reservation-list{padding:4px 14px 10px;max-height:220px;overflow-y:auto;}
.res-row{display:flex;align-items:center;gap:8px;padding:7px 0;border-bottom:1px solid #f5f5f5;font-size:.8rem;}
.res-row:last-child{border-bottom:none;}
.res-loc{font-family:"SF Mono","Roboto Mono",monospace;font-weight:700;color:#222;min-width:80px;}
.res-sku{color:#888;flex:1;font-size:.75rem;}
.res-tote{font-size:.72rem;color:#f9ab00;font-weight:600;background:#fff8e1;padding:1px 6px;border-radius:10px;}
.res-del{background:none;border:none;color:#e57373;font-size:1rem;cursor:pointer;padding:2px 4px;line-height:1;}
.res-del:hover{color:#c62828;}
.res-empty{color:#bbb;font-size:.82rem;padding:8px 0;text-align:center;}
#print-label{display:none;}
</style>
</head>
<body>

<div id="header">
  <span class="title">🏷 Putaway</span>
  <select id="wh-select" class="hdr-select" onchange="onWhChange()">
    <option value="001">001 — LA</option>
    <option value="002" selected>002 — NJ</option>
    <option value="101">101 — ON</option>
  </select>
  <select id="zone-select" class="hdr-select visible njfc" onchange="onZoneChange()">
    <option value="">All Zones</option>
    <option value="NJFC" selected>NJFC</option>
    <option value="SFC">SFC</option>
  </select>
  <button id="continuous-toggle" onclick="toggleContinuous()" title="Continuous mode: recommend nearby locations">📍 Off</button>
</div>

<div id="tote-bar">
  <span class="tote-label">EXT Tote</span>
  <input id="tote-input" type="text" inputmode="text"
         placeholder="Scan tote… (optional)"
         autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false">
  <button id="tote-clear" onclick="clearTote()">✕</button>
  <span id="tote-mode-badge" class="tote-mode-badge badge-lookup">Lookup</span>
</div>

<div id="search-area">
  <div class="search-wrap">
    <span class="search-icon">🔍</span>
    <input id="upc-input" type="text" inputmode="text"
           placeholder="Scan or type UPC / SKU…"
           autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false">
    <button id="clear-btn" onclick="clearSearch()">✕</button>
  </div>
</div>

<div id="result-area">
  <div class="hint">
    <div class="icon">📦</div>
    <p>Scan a tote to start putaway<br>or scan a SKU to look up inventory</p>
  </div>
</div>

<!-- Reservation Panel -->
<div id="reservation-panel">
  <div id="reservation-toggle" onclick="toggleReservations()">
    <span id="reservation-title">📋 Reserved Locations</span>
    <span id="reservation-count-badge"></span>
    <span id="reservation-chevron">▶</span>
  </div>
  <div id="reservation-list" style="display:none;"></div>
</div>

<div id="print-label">
  <div id="print-loc" style="font-family:monospace;font-size:1.8rem;font-weight:900;letter-spacing:2px;color:#000;"></div>
</div>

<script>
// ── State ──────────────────────────────────────────────────────────────────
let currentWh   = "002";
let currentZone = "NJFC";   // default 002 = NJFC
let currentTote = "";
let currentSku  = "";        // set on each lookup result
let currentBannerSource = ""; // set on each lookup result
let currentContinuous = false; // continuous mode toggle
let lastQuery   = "";
let debounce = null, bgBuffer = "", bgTimer = null;
const SCAN_DELAY = 300;

const upcInput      = document.getElementById("upc-input");
const clearBtn      = document.getElementById("clear-btn");
const resultArea    = document.getElementById("result-area");
const whSelect      = document.getElementById("wh-select");
const zoneSelect    = document.getElementById("zone-select");
const toteInput     = document.getElementById("tote-input");
const toteClear     = document.getElementById("tote-clear");
const toteModeBadge = document.getElementById("tote-mode-badge");

// ── Warehouse ──────────────────────────────────────────────────────────────
function onWhChange() {
  currentWh = whSelect.value;
  if (currentWh === "002") {
    zoneSelect.classList.add("visible");
  } else {
    zoneSelect.classList.remove("visible");
    currentZone = ""; zoneSelect.value = "";
    zoneSelect.classList.remove("njfc","sfc");
  }
  if (lastQuery) doLookup(lastQuery);
}

// ── Continuous mode ────────────────────────────────────────────────────────
function toggleContinuous() {
  currentContinuous = !currentContinuous;
  const btn = document.getElementById("continuous-toggle");
  btn.textContent = currentContinuous ? "📍 Continuous" : "📍 Off";
  btn.classList.toggle("on", currentContinuous);
}

// ── Zone ───────────────────────────────────────────────────────────────────
function onZoneChange() {
  currentZone = zoneSelect.value;
  zoneSelect.classList.remove("njfc","sfc");
  if (currentZone === "NJFC") zoneSelect.classList.add("njfc");
  if (currentZone === "SFC")  zoneSelect.classList.add("sfc");
  if (lastQuery) doLookup(lastQuery);
}

// ── Tote ───────────────────────────────────────────────────────────────────
function setTote(val, requery) {
  currentTote = val.trim().toUpperCase();
  toteInput.value = currentTote;
  toteInput.classList.add("confirmed");
  toteClear.classList.add("visible");
  toteModeBadge.textContent = "Putaway";
  toteModeBadge.className = "tote-mode-badge badge-putaway";
  if (requery && lastQuery) doLookup(lastQuery);
}
function clearTote() {
  currentTote = ""; toteInput.value = "";
  toteInput.classList.remove("confirmed");
  toteClear.classList.remove("visible");
  toteModeBadge.textContent = "Lookup";
  toteModeBadge.className = "tote-mode-badge badge-lookup";
  if (lastQuery) doLookup(lastQuery);
}
function clearSearch() {
  upcInput.value = ""; lastQuery = "";
  clearBtn.classList.remove("visible");
  resultArea.innerHTML = `<div class="hint"><div class="icon">📦</div><p>Scan a tote to start putaway<br>or scan a SKU to look up inventory</p></div>`;
}

// ── Inputs ─────────────────────────────────────────────────────────────────
toteInput.addEventListener("keydown", function(e) {
  if (e.key === "Enter") {
    e.stopPropagation();
    const v = this.value.trim();
    if (v) setTote(v, false);  // confirm tote, don't re-query yet
    upcInput.focus();
  }
});
// Confirm tote on blur (tap away) — no re-query, user will scan SKU next
toteInput.addEventListener("blur", function() {
  const v = this.value.trim();
  if (v && v !== currentTote) setTote(v, false);
});
upcInput.addEventListener("input", function() {
  const v = this.value.trim();
  clearBtn.classList.toggle("visible", v.length > 0);
  clearTimeout(debounce);
  if (v.length < 3) return;
  debounce = setTimeout(() => { lastQuery = v; doLookup(v); }, 600);
});
upcInput.addEventListener("keydown", function(e) {
  if (e.key === "Enter") {
    e.stopPropagation(); clearTimeout(debounce);
    const v = this.value.trim();
    if (v.length >= 3) { lastQuery = v; doLookup(v); }
  }
});

// ── Background scanner — EXT* → tote, else → UPC ──────────────────────────
document.addEventListener("keydown", function(e) {
  if (e.ctrlKey || e.altKey || e.metaKey) return;
  if (e.target === upcInput || e.target === toteInput) return;
  if (e.key === "Enter") {
    clearTimeout(bgTimer);
    const v = bgBuffer.trim(); bgBuffer = "";
    if (v.length >= 3) {
      if (v.toUpperCase().startsWith("EXT")) { setTote(v, false); upcInput.focus(); }
      else { upcInput.value = v; clearBtn.classList.add("visible"); lastQuery = v; doLookup(v); }
    }
    return;
  }
  if (e.key.length === 1) {
    bgBuffer += e.key; clearTimeout(bgTimer);
    bgTimer = setTimeout(() => {
      const v = bgBuffer.trim(); bgBuffer = "";
      if (v.length >= 3) {
        if (v.toUpperCase().startsWith("EXT")) { setTote(v, false); upcInput.focus(); }
        else { upcInput.value = v; clearBtn.classList.add("visible"); lastQuery = v; doLookup(v); }
      }
    }, SCAN_DELAY);
  }
});

// ── API ────────────────────────────────────────────────────────────────────
async function doLookup(upc) {
  showLoading();
  upcInput.blur(); toteInput.blur();
  try {
    const res = await fetch(`/api/lookup?upc=${encodeURIComponent(upc)}&wh=${encodeURIComponent(currentWh)}&zone=${encodeURIComponent(currentZone)}&tote=${encodeURIComponent(currentTote)}&continuous=${currentContinuous ? '1' : '0'}`);
    const data = await res.json();
    data.error ? showError(data.error) : renderResult(data);
  } catch(e) { showError("Network error: " + e.message); }
  upcInput.value = ""; clearBtn.classList.remove("visible");
}

// ── Render helpers ─────────────────────────────────────────────────────────
function zoneChip(label, big) {
  if (!label) return "";
  const base = label === "NJFC" ? "njfc" : label === "SFC" ? "sfc" : "def";
  if (big) return `<span class="dest-zone-chip ${base}">${label}</span>`;
  const cls = label === "NJFC" ? "zone-njfc" : label === "SFC" ? "zone-sfc" : "zone-def";
  return `<span class="zone-chip ${cls}">${label}</span>`;
}

function destBanner(type, loc, crossZone) {
  const isA = type === "assigned";
  const isEmptyLoc = loc.source === "local_rec" || loc.source === "empty_bin";
  // Color logic
  let bannerClass;
  if (isEmptyLoc) {
    bannerClass = "warn";   // red — empty location assignment
  } else if (crossZone) {
    bannerClass = "zone-cross";
  } else if (loc.zone_label === "NJFC") {
    bannerClass = "zone-njfc";
  } else if (loc.zone_label === "SFC") {
    bannerClass = "zone-sfc";
  } else {
    bannerClass = isA ? "assigned" : "current";
  }
  const label = isEmptyLoc ? "Recommend Empty Location"
              : isA        ? "Assigned Location"
              :               "Recommended Location";
  const mp = isA
    ? [loc.tote_id ? `Tote: ${loc.tote_id}` : "", loc.location_type || "", loc.problem_qty ? `Qty: ${loc.problem_qty}` : ""]
    : [loc.quantity != null ? `Qty: ${loc.quantity}` : "", loc.expire_date ? `Exp: ${loc.expire_date}` : ""];
  const meta = mp.filter(Boolean).join("  ·  ");
  const safeLocNo = (loc.location_no || "").replace(/'/g, "\\'");
  return `<div class="dest-banner ${bannerClass}">
    <div class="dest-left">
      <div class="dest-label">${label}</div>
      <div class="dest-loc">${loc.location_no || "—"}</div>
      ${meta ? `<div class="dest-meta">${meta}</div>` : ""}
    </div>
    <div class="dest-zone-wrap">
      ${zoneChip(loc.zone_label, true)}
      <button class="print-btn" onclick="printLabel('${safeLocNo}')">🖨 Print</button>
    </div>
  </div>`;
}

function printLabel(loc) {
  document.getElementById("print-loc").textContent = loc;
  window.print();
  // Reserve only for empty-location recommendations
  if ((currentBannerSource === "local_rec" || currentBannerSource === "empty_bin")
      && currentTote && currentSku && currentWh) {
    fetch('/api/reserve', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        tote_id: currentTote, sku: currentSku,
        location_no: loc, wh: currentWh, zone_label: currentZone
      })
    }).then(() => loadReservations()).catch(e => console.warn('reserve failed', e));
  }
}

function extToteStrip(totes, activeTote) {
  if (!totes || totes.length === 0) return "";
  const chips = totes.map(t => {
    const isActive = activeTote && t.tote_id === activeTote;
    return `<span class="tote-chip${isActive ? " active" : ""}" onclick="setTote('${t.tote_id}')">
      <span class="tote-id">${t.tote_id}</span>
      <span class="tote-qty">${t.total_qty} pcs</span>
    </span>`;
  }).join("");
  return `<div class="tote-strip"><span class="tote-strip-label">EXT Totes</span><div class="tote-chips">${chips}</div></div>`;
}

function locRow(loc) {
  const isP = loc.source === "pending";
  const mp = isP
    ? [loc.tote_id ? `Tote: ${loc.tote_id}` : "", loc.location_type || "", loc.problem_qty ? `Qty: ${loc.problem_qty}` : ""]
    : [loc.quantity != null ? `Qty: ${loc.quantity}` : "",
       loc.expire_date ? `<span class="${loc.days_to_expire != null && loc.days_to_expire <= 30 ? "exp-soon" : ""}">Exp: ${loc.expire_date}${loc.days_to_expire != null ? " (" + loc.days_to_expire + "d)" : ""}</span>` : ""];
  const meta = mp.filter(Boolean).join("  ·  ");
  return `<div class="loc-row"><span class="loc-no">${loc.location_no}</span>${zoneChip(loc.zone_label)}${meta ? `<span class="loc-meta">${meta}</span>` : ""}</div>`;
}

function sectionCard(title, dotCls, rows) {
  return `<div class="section-card">
    <div class="section-header">
      <div class="section-dot ${dotCls}"></div>
      <span class="section-title">${title}</span>
      <span class="section-count">${rows.length} loc${rows.length !== 1 ? "s" : ""}</span>
    </div>
    <div class="loc-list">${rows.map(locRow).join("")}</div>
  </div>`;
}

// ── Main render ────────────────────────────────────────────────────────────
function renderResult(d) {
  currentSku = d.item_number || "";
  currentBannerSource = d.banner_source || "";
  const name = d.goods_en_name || d.goods_name || "—";
  let html = `<div class="product-pill"><span class="sku-badge">${d.item_number}</span><span class="product-name">${name}</span></div>`;

  if (d.mode === "lookup") {
    html += extToteStrip(d.ext_totes, null);
    const rows = d.current || [];
    html += rows.length > 0
      ? sectionCard("Current Inventory", "dot-current", rows)
      : `<div class="section-card"><div class="unavailable">No inventory found${d.zone_filter ? " in " + d.zone_filter : ""}</div></div>`;
    resultArea.innerHTML = html;
    return;
  }

  // Putaway mode
  const src      = d.banner_source || "none";
  const banners  = d.banner_rows   || [];
  const currRows = d.current       || [];

  if (d.not_in_tote) {
    // Big warning banner — same visual weight as dest-banner
    html += `<div class="dest-banner warn">
      <div class="dest-left">
        <div class="dest-label">Not In Tote</div>
        <div class="dest-loc" style="font-size:1.3rem;color:#c62828;">Scan item into ${d.tote} first</div>
        <div class="dest-meta">This item has no record in ${d.tote} — check the EXT totes below</div>
      </div>
      <div class="dest-zone-wrap">
        <span class="dest-zone-chip def" style="background:#fde8e8;color:#c62828;border-color:#e57373;font-size:1.4rem;">⚠️</span>
      </div>
    </div>`;
    html += extToteStrip(d.ext_totes, null);
    html += currRows.length > 0
      ? sectionCard("Current Inventory", "dot-current", currRows)
      : `<div class="section-card"><div class="unavailable">No inventory found${d.zone_filter ? " in " + d.zone_filter : ""}</div></div>`;
  } else {
    if (d.wrong_zone && d.assigned) {
      const assignedLoc  = d.assigned.location_no;
      const assignedZone = d.assigned.zone_label || "another zone";
      if (d.banner_source === "pending") {
        html += `<div class="notice-bar">ℹ️ No stock in <b>${d.zone_filter}</b> — using assigned location <b>${assignedLoc}</b> (${assignedZone})</div>`;
      } else {
        html += `<div class="notice-bar">ℹ️ EXT putaway assigned to <b>${assignedLoc}</b> (${assignedZone}) — showing ${d.zone_filter} locations instead</div>`;
      }
    }
    if (d.banner_source === "current_other_zone") {
      const otherZone = banners.length > 0 ? (banners[0].zone_label || "another zone") : "another zone";
      html += `<div class="notice-bar">ℹ️ No stock in <b>${d.zone_filter || "selected zone"}</b> — showing <b>${otherZone}</b> locations instead</div>`;
    }
    if (d.banner_source === "empty_bin" || d.banner_source === "local_rec") {
      html += `<div class="notice-bar">ℹ️ No existing stock — showing empty location recommendation</div>`;
    }    if (banners.length > 0) {
      // cross-zone: wrong_zone fallback to assigned in other zone, OR no-zone-filter result shown under a zone selection
      const bannerZone  = banners[0].zone_label || "";
      const isCrossZone = d.zone_filter && bannerZone && bannerZone !== d.zone_filter;
      html += destBanner(src === "pending" ? "assigned" : "current", banners[0], isCrossZone);
    } else {
      html += `<div class="section-card"><div class="unavailable">⚠️ No locations available${d.zone_filter ? " in " + d.zone_filter : ""}</div></div>`;
    }
    html += extToteStrip(d.ext_totes, d.tote);
    const bannerLoc = banners.length > 0 ? banners[0].location_no : null;
    const otherRows = currRows.filter(r => r.location_no !== bannerLoc);
    if (otherRows.length > 0) html += sectionCard("Other Locations", "dot-current", otherRows);
  }

  resultArea.innerHTML = html;
}

function showLoading() { resultArea.innerHTML = `<div class="spinner-wrap"><div class="spinner"></div>Looking up…</div>`; }
function showError(err) {
  const msgs = { item_not_found: "❌ UPC / SKU not found", invalid_warehouse: "❌ Invalid warehouse" };
  resultArea.innerHTML = `<div class="error-msg">${msgs[err] || "❌ " + err}</div>`;
}

// ── Reservation Panel ─────────────────────────────────────────────────────
let reservationOpen = false;

async function loadReservations() {
  try {
    const res = await fetch(`/api/reservations?wh=${encodeURIComponent(currentWh)}`);
    const data = await res.json();
    const rows = data.reservations || [];
    const badge = document.getElementById("reservation-count-badge");
    const list  = document.getElementById("reservation-list");
    // Update count badge
    if (rows.length > 0) {
      badge.textContent = rows.length;
      badge.style.display = "inline-block";
    } else {
      badge.style.display = "none";
    }
    // Render list if open
    if (reservationOpen) {
      if (rows.length === 0) {
        list.innerHTML = `<div class="res-empty">No active reservations</div>`;
      } else {
        list.innerHTML = rows.map(r => `
          <div class="res-row" id="res-row-${r.id}">
            <span class="res-loc">${r.location_no}</span>
            <span class="res-sku">${r.sku}</span>
            <span class="res-tote">${r.tote_id}</span>
            <button class="res-del" onclick="deleteReservation(${r.id},'${r.tote_id}','${r.sku}')" title="Remove">✕</button>
          </div>`).join("");
      }
    }
  } catch(e) { console.warn("loadReservations failed", e); }
}

function toggleReservations() {
  reservationOpen = !reservationOpen;
  const list = document.getElementById("reservation-list");
  const chevron = document.getElementById("reservation-chevron");
  list.style.display = reservationOpen ? "block" : "none";
  chevron.classList.toggle("open", reservationOpen);
  if (reservationOpen) loadReservations();
}

async function deleteReservation(id, toteId, sku) {
  // Delete single row via a dedicated endpoint, or release the whole tote+sku
  try {
    const res = await fetch(`/api/reserve/row/${id}`, { method: "DELETE" });
    const data = await res.json();
    if (data.ok) {
      const row = document.getElementById(`res-row-${id}`);
      if (row) row.remove();
      await loadReservations();
    }
  } catch(e) { console.warn("deleteReservation failed", e); }
}

// Refresh badge count after every lookup
const _origRenderResult = renderResult;

// Initial focus on tote input
toteInput.focus();
// Load reservation count on startup
loadReservations();
</script>
</body>
</html>
"""
