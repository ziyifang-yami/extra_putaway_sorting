"""
Putaway Location Reservation
Local SQLite store for in-session location pre-occupancy.
"""

import logging
import sqlite3
import time
from pathlib import Path

from sqlalchemy import text

log = logging.getLogger("putaway.reservation")


class ReservationStore:
    def __init__(self, db_path: str):
        self._db_path = db_path
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS reservations (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    tote_id       TEXT    NOT NULL,
                    sku           TEXT    NOT NULL,
                    location_no   TEXT    NOT NULL,
                    wh            TEXT    NOT NULL,
                    zone_label    TEXT    NOT NULL DEFAULT '',
                    created_at    REAL    NOT NULL,
                    released_at   REAL,
                    UNIQUE(tote_id, sku, wh)
                );
                CREATE INDEX IF NOT EXISTS idx_res_wh
                    ON reservations(wh, released_at);
                CREATE INDEX IF NOT EXISTS idx_res_tote_sku
                    ON reservations(tote_id, sku, released_at);
            """)
        log.info(f"ReservationStore initialized at {self._db_path}")

    def reserve(self, tote_id: str, sku: str, location_no: str,
                wh: str, zone_label: str) -> dict:
        """Upsert reservation. Returns {reservation_id, created_at}."""
        now = time.time()
        with self._connect() as conn:
            conn.execute("""
                INSERT INTO reservations (tote_id, sku, location_no, wh, zone_label, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(tote_id, sku, wh) DO UPDATE SET
                    location_no = excluded.location_no,
                    zone_label  = excluded.zone_label,
                    created_at  = excluded.created_at,
                    released_at = NULL
            """, (tote_id, sku, location_no, wh, zone_label, now))
            row = conn.execute(
                "SELECT id, created_at FROM reservations "
                "WHERE tote_id=? AND sku=? AND wh=? AND released_at IS NULL",
                (tote_id, sku, wh)
            ).fetchone()
        import datetime
        created = datetime.datetime.fromtimestamp(row["created_at"]).isoformat()
        return {"reservation_id": row["id"], "created_at": created}

    def get_location_for_sku(self, sku: str, wh: str,
                             zone_label: str = "") -> str | None:
        """
        Return most recent active reservation location for this SKU+wh.
        If zone_label is given, only return if zone matches.
        """
        with self._connect() as conn:
            row = conn.execute("""
                SELECT location_no, zone_label FROM reservations
                WHERE sku = ? AND wh = ? AND released_at IS NULL
                ORDER BY created_at DESC LIMIT 1
            """, (sku, wh)).fetchone()
        if row is None:
            return None
        # Zone check: if caller specifies zone, must match
        if zone_label and row["zone_label"] and row["zone_label"] != zone_label:
            return None
        return row["location_no"]

    def get_latest_reserved_location(self, wh: str, zone_label: str = "") -> str | None:
        """Return the most recently reserved location_no (anchor for proximity sorting)."""
        with self._connect() as conn:
            if zone_label:
                row = conn.execute("""
                    SELECT location_no FROM reservations
                    WHERE wh = ? AND zone_label = ? AND released_at IS NULL
                    ORDER BY created_at DESC LIMIT 1
                """, (wh, zone_label)).fetchone()
            else:
                row = conn.execute("""
                    SELECT location_no FROM reservations
                    WHERE wh = ? AND released_at IS NULL
                    ORDER BY created_at DESC LIMIT 1
                """, (wh,)).fetchone()
        return row["location_no"] if row else None

    def get_session_occupancy(self, wh: str) -> dict[str, set[str]]:        """
        Return {location_no: {sku1, sku2, ...}} for all active reservations in wh.
        Used to compute effective_item_count for can_random locations.
        """
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT location_no, sku FROM reservations
                WHERE wh = ? AND released_at IS NULL
            """, (wh,)).fetchall()
        occupancy: dict[str, set[str]] = {}
        for row in rows:
            occupancy.setdefault(row["location_no"], set()).add(row["sku"])
        return occupancy

    def release_by_tote(self, tote_id: str) -> int:
        """Manual release of all reservations for a tote."""
        now = time.time()
        with self._connect() as conn:
            cur = conn.execute("""
                UPDATE reservations SET released_at = ?
                WHERE tote_id = ? AND released_at IS NULL
            """, (now, tote_id))
        return cur.rowcount

    def release_stale(self, engine) -> list[tuple[str, str]]:
        """
        For each active (tote_id, sku): check wh_pending_item.
        If SUM(problem_qty) == SUM(done_qty) or no rows → release.
        Returns list of (tote_id, sku) released.
        """
        with self._connect() as conn:
            active = conn.execute("""
                SELECT DISTINCT tote_id, sku, wh FROM reservations
                WHERE released_at IS NULL
            """).fetchall()

        released = []
        sql = text("""
            SELECT COALESCE(SUM(problem_qty), 0) AS total,
                   COALESCE(SUM(done_qty), 0)    AS done
            FROM yamibuy_wh.wh_pending_item
            WHERE target      = :tote
              AND item_number = :sku
              AND status      IN (0, 1)
        """)
        now = time.time()
        for row in active:
            tote_id, sku, wh = row["tote_id"], row["sku"], row["wh"]
            try:
                with engine.connect() as c:
                    result = c.execute(sql, {"tote": tote_id, "sku": sku}).fetchone()
                if result is None or result[0] == result[1]:
                    with self._connect() as conn:
                        conn.execute("""
                            UPDATE reservations SET released_at = ?
                            WHERE tote_id = ? AND sku = ? AND released_at IS NULL
                        """, (now, tote_id, sku))
                    released.append((tote_id, sku))
                    log.info(f"Released reservation: tote={tote_id} sku={sku}")
            except Exception as e:
                log.warning(f"release_stale error for {tote_id}/{sku}: {e}")

        return released

    def list_active(self, wh: str) -> list[dict]:
        """Debug: return all active reservations for a warehouse."""
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT id, tote_id, sku, location_no, wh, zone_label, created_at
                FROM reservations
                WHERE wh = ? AND released_at IS NULL
                ORDER BY created_at DESC
            """, (wh,)).fetchall()
        import datetime
        return [
            {
                "id": r["id"], "tote_id": r["tote_id"], "sku": r["sku"],
                "location_no": r["location_no"], "wh": r["wh"],
                "zone_label": r["zone_label"],
                "created_at": datetime.datetime.fromtimestamp(r["created_at"]).isoformat(),
            }
            for r in rows
        ]


class ReservationPoller:
    """
    Session-based reconciliation. No background threads.
    Calls release_stale() once at the start of each new session
    (defined as first lookup after idle_timeout seconds of inactivity).
    """

    def __init__(self, store: ReservationStore, engine,
                 idle_timeout: int = 300):
        self._store = store
        self._engine = engine
        self._idle_timeout = idle_timeout
        self._last_activity: float = 0.0

    def on_lookup(self) -> None:
        """Call at the start of every GET /api/lookup."""
        now = time.time()
        if now - self._last_activity > self._idle_timeout:
            log.info("New session detected — running reservation reconciliation")
            try:
                released = self._store.release_stale(self._engine)
                if released:
                    log.info(f"Session reconciliation released: {released}")
            except Exception as e:
                log.warning(f"Session reconciliation error: {e}")
        self._last_activity = now
