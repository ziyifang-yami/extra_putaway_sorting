# Design Document: Putaway Location Reservation

## Overview

Replace the WMS LR API fallback with a self-contained SQL-based recommendation engine.

**Problem**: During continuous label printing, the LR API recommends the same empty location repeatedly because WMS DB hasn't been updated yet (items not yet physically putaway).

**Solution**:
1. Local SQLite DB tracks "in-session" location reservations — written on Print, released when SKU is fully putaway from the EXT tote
2. SQL-based empty bin recommendation that respects these reservations
3. Session-based reconciliation (no background threads)

Zero dependency on WMS LR API.

---

## Architecture

```
GET /api/lookup
  ├─ Step 1: wh_pending_item → assigned location (existing, unchanged)
  ├─ Step 2: wh_lot_inventory → current stock in zone (existing, unchanged)
  ├─ Step 3: wh_lot_inventory → current stock other zone (existing, unchanged)
  ├─ Step 4: reservations.db → reuse same-SKU reservation (NEW)
  ├─ Step 5: SQL recommendation → get_recommended_bins() (NEW, replaces LR API)
  └─ Step 6: WMS queryAvaiBinList → last resort (existing)

POST /api/reserve  ← frontend calls on Print (only for local_rec / empty_bin)
  → upsert into reservations.db

ReservationPoller.on_lookup()  ← called at start of every GET /api/lookup
  → if new session (> 5 min idle): run release_stale() synchronously
  → else: no-op
```

---

## Components and Interfaces

### ReservationStore

Wraps a local SQLite `reservations.db` file.

```python
class ReservationStore:
    def reserve(self, tote_id: str, sku: str, location_no: str,
                wh: str, zone_label: str) -> dict:
        """Upsert reservation. Returns {reservation_id, created_at}."""

    def get_location_for_sku(self, sku: str, wh: str,
                             zone_label: str) -> str | None:
        """
        Return most recent active reservation location for this SKU+wh+zone.
        Returns None if no active reservation or zone doesn't match.
        Used for Step 4 reuse.
        """

    def get_session_occupancy(self, wh: str) -> dict[str, set[str]]:
        """
        Return {location_no: {sku1, sku2, ...}} for all active reservations.
        Used by Step 5 Python post-filter to compute effective_item_count.
        """

    def release_stale(self, engine) -> list[tuple[str, str]]:
        """
        For each active (tote_id, sku): check wh_pending_item.
        If SUM(problem_qty) == SUM(done_qty) or no rows → release.
        Returns list of (tote_id, sku) released.
        """

    def release_by_tote(self, tote_id: str) -> int:
        """Manual release of all reservations for a tote. Returns rows affected."""
```

### ReservationPoller

No background thread. Session-based, called from each lookup request.

```python
class ReservationPoller:
    def __init__(self, store: ReservationStore, engine, idle_timeout: int = 300):
        ...

    def on_lookup(self) -> None:
        """
        Call at start of GET /api/lookup.
        If time since last lookup > idle_timeout → new session → run release_stale().
        Otherwise → no-op.
        """
```

---

## Data Models

### SQLite Schema

```sql
CREATE TABLE IF NOT EXISTS reservations (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    tote_id       TEXT    NOT NULL,
    sku           TEXT    NOT NULL,
    location_no   TEXT    NOT NULL,
    wh            TEXT    NOT NULL,
    zone_label    TEXT    NOT NULL DEFAULT '',
    created_at    REAL    NOT NULL,   -- unix timestamp (time.time())
    released_at   REAL,              -- NULL = active
    UNIQUE(tote_id, sku, wh)         -- one reservation per tote+SKU+wh
);

CREATE INDEX IF NOT EXISTS idx_res_wh      ON reservations(wh, released_at);
CREATE INDEX IF NOT EXISTS idx_res_tote_sku ON reservations(tote_id, sku, released_at);
```

---

## Core SQL Queries

### Step 1a — size_id by historical binding (preferred)

```sql
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
```

### Step 1b — size_id by volume (fallback for new SKUs)

`tote_qty` = `total_qty` for this SKU from `ext_totes` context (default 1 if unknown).

```sql
SELECT s.size_id
FROM yamibuy_wh.wh_location_size s
WHERE s.volume >= (
    SELECT (CASE WHEN g.volume IS NULL OR g.volume = 0 THEN 50 ELSE g.volume END)
           * :tote_qty
    FROM yamibuy_im.im_item i
    INNER JOIN yamibuy_master.xysc_wearhouse_goods g ON i.goods_id = g.goods_id
    WHERE i.item_number = :sku
)
  AND s.description  != 'High Value'
  AND s.location_type = 4
ORDER BY s.volume ASC
LIMIT 1
```

### Step 2 — Available bins for resolved size_id

Zone filter injected at Python level:
- NJFC: `AND l.location_no NOT LIKE 'S%'`
- SFC: `AND l.location_no LIKE 'S%'`
- All zones: (omitted)

```sql
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
      l.item_count = 0          -- completely empty
      OR sli.rec_id IS NOT NULL -- already has this SKU bound
      OR l.can_random = 1       -- allows mixing (effective_item_count checked in Python)
  )
  -- {zone_filter}              -- injected at Python level
HAVING capacity >= 1
ORDER BY has_bind DESC, capacity ASC
LIMIT 50
```

**Python post-filter for `can_random` locations** (using `get_session_occupancy()`):

```python
occupancy = reservation_store.get_session_occupancy(wh)
# occupancy = {"C11872C": {"SKU-A"}, "B20636": {"SKU-A", "SKU-B"}}

results = []
for row in raw_rows:
    loc_no = row["location_no"]

    if row["item_count"] > 0 and row["has_bind"] == 0 and row["can_random"]:
        # Mixed location: check effective capacity
        session_other_skus = len(
            occupancy.get(loc_no, set()) - {sku}  # other SKUs on this loc
        )
        effective_item_count = row["item_count"] + session_other_skus
        if row["max_sku"] <= effective_item_count:
            continue  # no room

    results.append(row)
    if len(results) >= 5:
        break
```

---

## Lookup Algorithm (complete)

```
GET /api/lookup?upc=&wh=&zone=&tote=

[Session reconciliation]
reservation_poller.on_lookup()
  → if new session: release_stale() synchronously

[Data fetch]
item     = lookup_item(upc)
current  = get_current_locations(sku, wh, zone_id)
ext_totes = get_ext_tote_summary(sku, wh)
tote_qty  = ext_totes[tote].total_qty  (for current tote, or 1 if unknown)

IF no tote:
    → lookup mode (existing, unchanged)

IF tote:
    assigned     = get_tote_assigned_location(tote, sku, wh)
    tote_has_sku = any tote in ext_totes matches current tote

    ─────────────────────────────────────────────────
    STEP 1: assigned location exists
    ─────────────────────────────────────────────────
    IF assigned:
        IF zone_id set AND assigned.zone != selected_zone:
            wrong_zone = True
            IF current (in selected zone):
                banner = current, source = "current_zone"
            ELSE:
                banner = [assigned], source = "pending"  (cross-zone, yellow)
        ELSE:
            banner = [assigned], source = "pending"
        → DONE

    ─────────────────────────────────────────────────
    STEP 2+3: not_in_tote warning
    ─────────────────────────────────────────────────
    ELIF not tote_has_sku:
        not_in_tote = True
        IF current (selected zone): banner = current, source = "current_zone"
        ELIF current (all zones):   banner = current, source = "current_other_zone"
        ELSE: fallthrough to step 4–6 below
        → DONE (with warning banner)

    ─────────────────────────────────────────────────
    STEP 2: in tote, no assigned — try current stock
    ─────────────────────────────────────────────────
    ELSE:
        IF current (selected zone):   banner = current, source = "current_zone"  → DONE
        IF current (all zones):       banner = current, source = "current_other_zone"  → DONE

        ─────────────────────────────────────────────
        STEP 4: reuse SQLite reservation (same SKU)
        ─────────────────────────────────────────────
        reused = reservation_store.get_location_for_sku(sku, wh, zone_label)
        IF reused:
            banner = [build_dict(reused, zone_label)], source = "reserved"  → DONE

        ─────────────────────────────────────────────
        STEP 5: fresh SQL recommendation
        ─────────────────────────────────────────────
        occupancy = reservation_store.get_session_occupancy(wh)
        bins = get_recommended_bins(sku, wh, zone_label, tote_qty, occupancy)
        IF bins:
            banner = bins, source = "local_rec"  → DONE

        ─────────────────────────────────────────────
        STEP 6: WMS empty bin fallback
        ─────────────────────────────────────────────
        bins = wms_client.get_empty_bins(wh, zone_label)
        IF bins:
            banner = bins, source = "empty_bin"  → DONE

        ELSE:
            banner = [], source = "none"
```

---

## get_recommended_bins() Algorithm

```python
def get_recommended_bins(sku, wh, zone_label, tote_qty, occupancy, limit=5):
    # Step 1: resolve size_id
    size_id = get_size_id_by_history(sku, wh)
    if size_id is None:
        size_id = get_size_id_by_volume(sku, wh, tote_qty)
    if size_id is None:
        return []   # caller falls through to Step 6

    # Step 2: query bins
    zone_filter = build_zone_filter(zone_label)
    raw_rows = run_sql(STEP2_SQL, sku=sku, wh=wh, size_id=size_id,
                       zone_filter=zone_filter)

    # Step 3: Python post-filter for can_random locations
    results = []
    for row in raw_rows:
        loc_no = row["location_no"]
        if row["item_count"] > 0 and row["has_bind"] == 0 and row["can_random"]:
            session_other = len(occupancy.get(loc_no, set()) - {sku})
            if row["max_sku"] <= row["item_count"] + session_other:
                continue
        # zone label for 002/NJ
        actual_zone = "SFC" if loc_no.upper().startswith("S") else "NJFC" if wh == "002" else ""
        results.append({
            "location_no": loc_no, "zone_label": actual_zone,
            "quantity": 0, "source": "local_rec", ...
        })
        if len(results) >= limit:
            break

    return results
```

---

## Release Logic

### Per (tote_id, sku) granularity

```
FOR each active (tote_id, sku) in reservations:
    SELECT SUM(problem_qty) AS total, SUM(done_qty) AS done
    FROM wh_pending_item
    WHERE target = tote_id AND item_number = sku AND status IN (0, 1)

    IF total == done OR no rows:
        → SET released_at = now() for this (tote_id, sku)
        → other SKUs in same tote unaffected
```

### Session-based trigger (no background threads)

```
On GET /api/lookup:
    now = time.time()
    if now - last_activity > 300:       ← new session (> 5 min idle)
        run release_stale() synchronously
    last_activity = now

5 min idle → session ends silently, no action
Next lookup → new session → reconcile
```

---

## UI Changes

### Banner by banner_source

| source | Label | Color |
|--------|-------|-------|
| `pending` | Assigned Location | green/blue (same zone) or yellow (cross-zone) |
| `current_zone` | Recommended Location | green/blue |
| `current_other_zone` | Recommended Location | yellow |
| `reserved` | Recommended Location | green/blue (same zone) or yellow (cross-zone) |
| `local_rec` | **Recommend Empty Location** | **Red** |
| `empty_bin` | **Recommend Empty Location** | **Red** |

### Print → Reserve (only for empty-location modes)

```javascript
// New state variables
let currentSku = "";
let currentBannerSource = "";

// Set in renderResult(d):
currentSku = d.item_number;
currentBannerSource = d.banner_source;

// printLabel() — reserve only for empty-location recommendations
function printLabel(loc) {
    document.getElementById("print-loc").textContent = loc;
    window.print();

    if (currentBannerSource === "local_rec" || currentBannerSource === "empty_bin") {
        fetch('/api/reserve', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                tote_id: currentTote, sku: currentSku,
                location_no: loc, wh: currentWh, zone_label: currentZone
            })
        }).catch(e => console.warn('reserve failed', e));
    }
}
```

---

## New API Endpoints

### POST /api/reserve
```json
Request:  {"tote_id": "EXT0008", "sku": "1019167791", "location_no": "C11872C", "wh": "002", "zone_label": "NJFC"}
Response: {"ok": true, "reservation_id": 42, "created_at": "2026-09-04T10:30:00"}
```
Validation: `tote_id` starts with `EXT`, `wh` in WAREHOUSES.

### DELETE /api/reserve/{tote_id}
Manual release. `{"ok": true, "rows_released": 3}`

### GET /api/reservations?wh=002
Debug: list all active reservations.

---

## Files to Create / Modify

| File | Change |
|------|--------|
| `reservation.py` | **New** — `ReservationStore` + `ReservationPoller` |
| `server.py` | Add: init, `on_lookup()`, `/api/reserve` endpoints, `get_recommended_bins()`, `get_size_id_*()`, update lookup fallback chain, update `printLabel()` + state vars in HTML |
| `requirements.txt` | No new deps (`sqlite3` is stdlib) |
