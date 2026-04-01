"""
Cache Agoda: destination_key + property_id → bản ghi JSON.

- Nếu đã cấu hình Supabase/Postgres (giống market_db): lưu bảng `agoda_channel_hotel` + `agoda_channel_destination_meta` trên cloud — bền khi deploy Streamlit.
- Nếu chưa có DB: fallback file SQLite cạnh app (hoặc AGODA_CACHE_SQLITE_PATH / AGODA_CACHE_FORCE_SQLITE=1).
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from pathlib import Path
from urllib.parse import parse_qs, unquote_plus, urlparse

_default_agoda_sqlite = Path(__file__).resolve().parent / "agoda_channel_cache.sqlite"
_env_path = (os.environ.get("AGODA_CACHE_SQLITE_PATH") or "").strip()
AGODA_CACHE_DB_PATH = (
    Path(_env_path).expanduser().resolve()
    if _env_path
    else _default_agoda_sqlite.resolve()
)

# Trường ít đổi — khi merge, ưu tiên giá trị đã cache; giá & đánh giá lấy từ scrape mới.
AGODA_STATIC_FIELD_KEYS = frozenset(
    {
        "Tên khách sạn",
        "Địa chỉ",
        "Địa điểm nổi bật",
        "Hạng sao",
        "Vĩ độ",
        "Kinh độ",
        "Link khách sạn",
        "Mã Property Agoda",
        "Mã property (OTA)",
        "ID khách sạn Travel",
    }
)


def _force_sqlite() -> bool:
    return (os.environ.get("AGODA_CACHE_FORCE_SQLITE") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _use_postgres() -> bool:
    if _force_sqlite():
        return False
    try:
        import market_db as _mdb

        return bool(_mdb.db_ready()[0])
    except Exception:
        return False


def destination_key_from_agoda_url(url: str) -> str:
    """Khóa địa điểm ổn định từ query Agoda (city hoặc textToSearch + locale)."""
    if not url or not str(url).strip():
        return "agoda|empty"
    try:
        q = parse_qs(urlparse(url.strip()).query)
    except Exception:
        return "agoda|invalid"
    city = (q.get("city") or [""])[0].strip()
    tts_raw = (q.get("textToSearch") or [""])[0].strip()
    try:
        tts = unquote_plus(tts_raw).strip().lower()[:200]
    except Exception:
        tts = tts_raw.lower()[:200] if tts_raw else ""
    loc = (q.get("locale") or q.get("htmlLanguage") or ["vi-vn"])[0].strip().lower()
    if city.isdigit():
        return f"agoda|city={city}|loc={loc}"
    if tts:
        return f"agoda|txt={tts}|loc={loc}"
    return f"agoda|hash={hash(url) & 0xFFFFFFFF:08x}|loc={loc}"


def case_fingerprint_from_agoda_url(url: str) -> str:
    """Khóa case: ngày, phòng, khách — để debug/audit (cột trong DB)."""
    if not url:
        return ""
    try:
        q = parse_qs(urlparse(url.strip()).query)
    except Exception:
        return ""
    parts = []
    for k in ("checkIn", "checkOut", "rooms", "adults", "children", "los"):
        parts.append(f"{k}={(q.get(k) or [''])[0]}")
    return "|".join(parts)


def row_property_id(row: dict) -> str:
    return str((row or {}).get("Mã Property Agoda") or "").strip()


def _connect_sqlite(path: Path | None = None) -> sqlite3.Connection:
    p = path or AGODA_CACHE_DB_PATH
    conn = sqlite3.connect(str(p), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_agoda_cache_db(path: Path | None = None) -> None:
    if _use_postgres():
        import market_db as mdb

        conn = mdb.get_conn()
        try:
            mdb.ensure_agoda_channel_cache_tables(conn)
            conn.commit()
        finally:
            conn.close()
        return
    conn = _connect_sqlite(path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agoda_hotel_row (
                destination_key TEXT NOT NULL,
                property_id TEXT NOT NULL,
                case_fingerprint TEXT NOT NULL,
                row_json TEXT NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY (destination_key, property_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agoda_destination_meta (
                destination_key TEXT PRIMARY KEY,
                last_run_pages_scanned INTEGER NOT NULL,
                last_run_listing_pages INTEGER NOT NULL,
                last_run_hotels INTEGER NOT NULL,
                last_run_mode TEXT NOT NULL DEFAULT 'full',
                updated_at REAL NOT NULL
            )
            """
        )
        _migrate_sqlite_agoda_meta(conn)
        conn.commit()
    finally:
        conn.close()


def _migrate_sqlite_agoda_meta(conn: sqlite3.Connection) -> None:
    rows = conn.execute("PRAGMA table_info(agoda_destination_meta)").fetchall()
    if not rows:
        return
    names = {r[1] for r in rows}
    if "last_full_pages_scanned" in names and "last_run_pages_scanned" not in names:
        conn.execute(
            "ALTER TABLE agoda_destination_meta RENAME COLUMN last_full_pages_scanned TO last_run_pages_scanned"
        )
        conn.execute(
            "ALTER TABLE agoda_destination_meta RENAME COLUMN last_full_listing_pages TO last_run_listing_pages"
        )
        conn.execute(
            "ALTER TABLE agoda_destination_meta RENAME COLUMN last_full_hotels TO last_run_hotels"
        )
    if "last_run_mode" not in {r[1] for r in conn.execute("PRAGMA table_info(agoda_destination_meta)").fetchall()}:
        conn.execute(
            "ALTER TABLE agoda_destination_meta ADD COLUMN last_run_mode TEXT NOT NULL DEFAULT 'full'"
        )


def upsert_destination_run_meta(
    destination_key: str,
    pages_scanned: int,
    listing_pages_reported: int,
    hotels_count: int,
    run_mode: str,
    path: Path | None = None,
) -> None:
    if not destination_key or pages_scanned < 1:
        return
    rm = (run_mode or "full").strip().lower()
    if rm not in ("full", "update"):
        rm = "full"
    init_agoda_cache_db(path)
    if _use_postgres():
        import market_db as mdb

        conn = mdb.get_conn()
        try:
            mdb.ensure_agoda_channel_cache_tables(conn)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO agoda_channel_destination_meta (
                        destination_key, last_run_pages_scanned, last_run_listing_pages,
                        last_run_hotels, last_run_mode, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (destination_key) DO UPDATE SET
                        last_run_pages_scanned = EXCLUDED.last_run_pages_scanned,
                        last_run_listing_pages = EXCLUDED.last_run_listing_pages,
                        last_run_hotels = EXCLUDED.last_run_hotels,
                        last_run_mode = EXCLUDED.last_run_mode,
                        updated_at = NOW()
                    """,
                    (
                        destination_key,
                        int(pages_scanned),
                        int(max(listing_pages_reported, pages_scanned)),
                        int(hotels_count),
                        rm,
                    ),
                )
            conn.commit()
        finally:
            conn.close()
        return

    now = time.time()
    conn = _connect_sqlite(path)
    try:
        conn.execute(
            """
            INSERT INTO agoda_destination_meta (
                destination_key, last_run_pages_scanned, last_run_listing_pages,
                last_run_hotels, last_run_mode, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(destination_key) DO UPDATE SET
                last_run_pages_scanned = excluded.last_run_pages_scanned,
                last_run_listing_pages = excluded.last_run_listing_pages,
                last_run_hotels = excluded.last_run_hotels,
                last_run_mode = excluded.last_run_mode,
                updated_at = excluded.updated_at
            """,
            (
                destination_key,
                int(pages_scanned),
                int(max(listing_pages_reported, pages_scanned)),
                int(hotels_count),
                rm,
                now,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_destination_run_meta(destination_key: str, path: Path | None = None) -> dict | None:
    if not destination_key:
        return None
    init_agoda_cache_db(path)
    if _use_postgres():
        import market_db as mdb

        conn = mdb.get_conn()
        try:
            mdb.ensure_agoda_channel_cache_tables(conn)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT last_run_pages_scanned, last_run_listing_pages, last_run_hotels,
                           last_run_mode, updated_at
                    FROM agoda_channel_destination_meta WHERE destination_key = %s
                    """,
                    (destination_key,),
                )
                row = cur.fetchone()
            if not row:
                return None
            ts = row["updated_at"]
            if hasattr(ts, "timestamp"):
                ut = float(ts.timestamp())
            else:
                ut = float(ts)
            return {
                "pages_scanned": int(row["last_run_pages_scanned"]),
                "listing_pages": int(row["last_run_listing_pages"]),
                "hotels": int(row["last_run_hotels"]),
                "run_mode": str(row.get("last_run_mode") or "full"),
                "updated_at": ut,
            }
        finally:
            conn.close()

    conn = _connect_sqlite(path)
    try:
        cur = conn.execute(
            """
            SELECT last_run_pages_scanned, last_run_listing_pages, last_run_hotels,
                   last_run_mode, updated_at
            FROM agoda_destination_meta WHERE destination_key = ?
            """,
            (destination_key,),
        )
        row = cur.fetchone()
        if not row:
            return None
        if len(row) >= 5:
            return {
                "pages_scanned": int(row[0]),
                "listing_pages": int(row[1]),
                "hotels": int(row[2]),
                "run_mode": str(row[3] or "full"),
                "updated_at": float(row[4]),
            }
        return {
            "pages_scanned": int(row[0]),
            "listing_pages": int(row[1]),
            "hotels": int(row[2]),
            "run_mode": "full",
            "updated_at": float(row[3]),
        }
    finally:
        conn.close()


def get_destination_full_meta(destination_key: str, path: Path | None = None) -> dict | None:
    """Alias tương thích — dùng get_destination_run_meta."""
    return get_destination_run_meta(destination_key, path)


def upsert_destination_full_meta(
    destination_key: str,
    pages_scanned: int,
    listing_pages_reported: int,
    hotels_count: int,
    path: Path | None = None,
) -> None:
    """Alias: coi như lần chạy full (tương thích code cũ)."""
    upsert_destination_run_meta(
        destination_key,
        pages_scanned,
        listing_pages_reported,
        hotels_count,
        "full",
        path=path,
    )


def load_cache_map(destination_key: str, path: Path | None = None) -> dict[str, dict]:
    if not destination_key:
        return {}
    init_agoda_cache_db(path)
    if _use_postgres():
        import market_db as mdb

        conn = mdb.get_conn()
        try:
            mdb.ensure_agoda_channel_cache_tables(conn)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT property_id, row_json FROM agoda_channel_hotel WHERE destination_key = %s",
                    (destination_key,),
                )
                rows = cur.fetchall()
        finally:
            conn.close()
        out: dict[str, dict] = {}
        for r in rows:
            pid = str(r["property_id"] or "").strip()
            if not pid:
                continue
            js = r["row_json"]
            if isinstance(js, dict):
                out[pid] = dict(js)
            elif isinstance(js, str):
                try:
                    out[pid] = json.loads(js)
                except Exception:
                    continue
            else:
                try:
                    out[pid] = json.loads(json.dumps(js, ensure_ascii=False))
                except Exception:
                    continue
        return out

    conn = _connect_sqlite(path)
    try:
        cur = conn.execute(
            "SELECT property_id, row_json FROM agoda_hotel_row WHERE destination_key = ?",
            (destination_key,),
        )
        out = {}
        for pid, js in cur.fetchall():
            if not pid:
                continue
            try:
                out[str(pid)] = json.loads(js)
            except Exception:
                continue
        return out
    finally:
        conn.close()


def upsert_agoda_rows(
    destination_key: str,
    case_fingerprint: str,
    rows: list[dict],
    path: Path | None = None,
) -> int:
    if not destination_key or not rows:
        return 0
    init_agoda_cache_db(path)
    if _use_postgres():
        import market_db as mdb
        from psycopg2.extras import Json

        conn = mdb.get_conn()
        n = 0
        try:
            mdb.ensure_agoda_channel_cache_tables(conn)
            with conn.cursor() as cur:
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    pid = row_property_id(row)
                    if not pid:
                        continue
                    try:
                        cur.execute(
                            """
                            INSERT INTO agoda_channel_hotel (
                                destination_key, property_id, case_fingerprint, row_json, updated_at
                            )
                            VALUES (%s, %s, %s, %s, NOW())
                            ON CONFLICT (destination_key, property_id) DO UPDATE SET
                                case_fingerprint = EXCLUDED.case_fingerprint,
                                row_json = EXCLUDED.row_json,
                                updated_at = NOW()
                            """,
                            (destination_key, pid, case_fingerprint or "", Json(row)),
                        )
                        n += 1
                    except Exception:
                        continue
            conn.commit()
            return n
        finally:
            conn.close()

    now = time.time()
    conn = _connect_sqlite(path)
    n = 0
    try:
        for row in rows:
            if not isinstance(row, dict):
                continue
            pid = row_property_id(row)
            if not pid:
                continue
            try:
                js = json.dumps(row, ensure_ascii=False)
            except (TypeError, ValueError):
                continue
            conn.execute(
                """
                INSERT INTO agoda_hotel_row (destination_key, property_id, case_fingerprint, row_json, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(destination_key, property_id) DO UPDATE SET
                    case_fingerprint = excluded.case_fingerprint,
                    row_json = excluded.row_json,
                    updated_at = excluded.updated_at
                """,
                (destination_key, pid, case_fingerprint or "", js, now),
            )
            n += 1
        conn.commit()
        return n
    finally:
        conn.close()


def merge_agoda_static_from_cache(cached_row: dict, fresh_row: dict) -> dict:
    """Giữ các trường tĩnh từ cache; còn lại (giá, điểm, hủy, bữa sáng, điểm đến…) từ fresh."""
    if not isinstance(fresh_row, dict):
        return {}
    if not isinstance(cached_row, dict):
        return dict(fresh_row)
    out = dict(fresh_row)
    for k in AGODA_STATIC_FIELD_KEYS:
        v = cached_row.get(k)
        if v is not None and str(v).strip():
            out[k] = v
    return out
