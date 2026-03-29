import json
import os
import re
import socket
import unicodedata
from datetime import datetime
from difflib import SequenceMatcher
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse, urlunparse, quote

import psycopg2
from psycopg2.extras import Json, RealDictCursor


STOPWORDS = {
    "hotel", "khach", "san", "khachsan", "resort", "spa", "hostel", "villa",
    "apartment", "homestay", "the", "by", "at", "and", "de", "la",
}

CITY_WORDS = {
    "nha", "trang", "ha", "noi", "hanoi", "danang", "da", "nang", "hcm", "hcmc",
    "ho", "chi", "minh", "saigon", "da", "lat", "phu", "quoc", "vung", "tau",
    "hoi", "an", "hue", "halong", "ha", "long", "sapa", "can", "tho",
}


def _safe_streamlit_secrets() -> dict:
    try:
        import streamlit as st  # type: ignore

        return dict(st.secrets)
    except Exception:
        return {}


def _secret_str(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _db_config() -> dict:
    secrets = _safe_streamlit_secrets()
    cfg = {
        "database_url": _secret_str(os.getenv("DATABASE_URL") or secrets.get("DATABASE_URL", "")),
        "host": _secret_str(os.getenv("SUPABASE_DB_HOST") or secrets.get("SUPABASE_DB_HOST", "")),
        "port": _secret_str(os.getenv("SUPABASE_DB_PORT") or secrets.get("SUPABASE_DB_PORT", "")),
        "dbname": _secret_str(os.getenv("SUPABASE_DB_NAME") or secrets.get("SUPABASE_DB_NAME", "")),
        "user": _secret_str(os.getenv("SUPABASE_DB_USER") or secrets.get("SUPABASE_DB_USER", "")),
        "password": _secret_str(os.getenv("SUPABASE_DB_PASSWORD") or secrets.get("SUPABASE_DB_PASSWORD", "")),
        "sslmode": _secret_str(os.getenv("SUPABASE_DB_SSLMODE") or secrets.get("SUPABASE_DB_SSLMODE", "require")) or "require",
        "connect_timeout": _secret_str(
            os.getenv("SUPABASE_DB_CONNECT_TIMEOUT") or secrets.get("SUPABASE_DB_CONNECT_TIMEOUT", "15")
        )
        or "15",
    }
    return cfg


def _normalize_database_url(url: str) -> str:
    u = url.strip()
    if not u:
        return u
    if u.startswith("postgres://"):
        u = "postgresql://" + u[len("postgres://") :]
    if "sslmode=" not in u and "?" not in u:
        return u + "?sslmode=require"
    if "sslmode=" not in u and "?" in u:
        return u + "&sslmode=require"
    return u


def _ipv4_hostaddr(hostname: str) -> str | None:
    """IPv4 để tránh lỗi IPv6 trên một số môi trường (vd. Streamlit Cloud)."""
    if not hostname:
        return None
    try:
        return socket.gethostbyname(hostname)
    except OSError:
        pass
    try:
        infos = socket.getaddrinfo(hostname, None, socket.AF_INET, socket.SOCK_STREAM)
        if infos:
            return infos[0][4][0]
    except OSError:
        pass
    return None


def _pooler_rewrite_configured() -> bool:
    secrets = _safe_streamlit_secrets()
    r = _secret_str(os.getenv("SUPABASE_POOLER_REGION") or secrets.get("SUPABASE_POOLER_REGION", ""))
    h = _secret_str(os.getenv("SUPABASE_POOLER_HOST") or secrets.get("SUPABASE_POOLER_HOST", ""))
    return bool(r or h)


def _supabase_direct_to_pooler_dsn(dsn: str) -> str:
    """
    Streamlit Cloud thường không kết nối được db.*.supabase.co:5432 (IPv6).
    Nếu Secrets có SUPABASE_POOLER_REGION hoặc SUPABASE_POOLER_HOST, tự đổi URI
    sang Transaction pooler (port 6543, user postgres.<project_ref>).
    """
    secrets = _safe_streamlit_secrets()
    region = _secret_str(os.getenv("SUPABASE_POOLER_REGION") or secrets.get("SUPABASE_POOLER_REGION", ""))
    pooler_host = _secret_str(os.getenv("SUPABASE_POOLER_HOST") or secrets.get("SUPABASE_POOLER_HOST", ""))
    if not region and not pooler_host:
        return dsn
    u = urlparse(_normalize_database_url(dsn))
    hn = (u.hostname or "").lower()
    m = re.match(r"^db\.([a-z0-9]+)\.supabase\.co$", hn)
    if not m:
        return dsn
    if (u.port or 5432) != 5432:
        return dsn
    ref = m.group(1)
    host_out = pooler_host or f"aws-0-{region}.pooler.supabase.com"
    user_plain = unquote(u.username or "postgres")
    pooler_user = f"postgres.{ref}" if user_plain == "postgres" else user_plain
    pwd_plain = unquote(u.password or "")
    netloc = f"{quote(pooler_user, safe='')}:{quote(pwd_plain, safe='')}@{host_out}:6543"
    path = u.path if u.path and u.path != "" else "/postgres"
    if not path.startswith("/"):
        path = "/" + path
    q = u.query
    if "sslmode=" not in (q or ""):
        q = "sslmode=require" + (f"&{q}" if q else "")
    return urlunparse(("postgresql", netloc, path, "", q, ""))


def _supabase_direct_db_host(hostname: str | None) -> bool:
    if not hostname:
        return False
    h = hostname.lower()
    return h.endswith(".supabase.co") and h.startswith("db.")


def _connect_kw_from_database_url_ipv4(dsn: str, timeout: int) -> dict[str, Any] | None:
    """Dùng host + hostaddr (IPv4) cho URI trỏ tới db.*.supabase.co."""
    u = urlparse(_normalize_database_url(dsn))
    hn = u.hostname
    if not _supabase_direct_db_host(hn or ""):
        return None
    v4 = _ipv4_hostaddr(hn or "")
    if not v4:
        return None
    qs = parse_qs(u.query)
    sslmode = (qs.get("sslmode") or ["require"])[0]
    user = unquote(u.username or "")
    password = unquote(u.password or "")
    dbname = (u.path or "/postgres").strip("/") or "postgres"
    port = u.port or 5432
    return {
        "host": hn,
        "hostaddr": v4,
        "port": port,
        "dbname": dbname,
        "user": user,
        "password": password,
        "sslmode": sslmode,
        "connect_timeout": timeout,
        "cursor_factory": RealDictCursor,
    }


def db_ready() -> tuple[bool, str]:
    cfg = _db_config()
    if cfg["database_url"]:
        return True, "DATABASE_URL"
    needed = ["host", "port", "dbname", "user", "password"]
    missing = [k for k in needed if not cfg[k]]
    if missing:
        return False, f"Missing DB config: {', '.join(missing)}"
    return True, f"{cfg['host']}:{cfg['port']}/{cfg['dbname']}"


def get_conn():
    ok, msg = db_ready()
    if not ok:
        raise RuntimeError(f"Supabase DB chưa cấu hình: {msg}")
    cfg = _db_config()
    try:
        timeout = max(5, int(cfg["connect_timeout"] or "15"))
    except ValueError:
        timeout = 15
    if cfg["database_url"]:
        dsn = _normalize_database_url(cfg["database_url"])
        dsn = _supabase_direct_to_pooler_dsn(dsn)
        kw_url = _connect_kw_from_database_url_ipv4(dsn, timeout)
        if kw_url:
            return psycopg2.connect(**kw_url)
        return psycopg2.connect(dsn, cursor_factory=RealDictCursor, connect_timeout=timeout)
    kw: dict[str, Any] = {
        "host": cfg["host"],
        "port": int(cfg["port"]),
        "dbname": cfg["dbname"],
        "user": cfg["user"],
        "password": cfg["password"],
        "sslmode": cfg["sslmode"],
        "connect_timeout": timeout,
        "cursor_factory": RealDictCursor,
    }
    if _supabase_direct_db_host(cfg["host"]) and _pooler_rewrite_configured():
        uq = quote(cfg["user"], safe="")
        pq = quote(cfg["password"], safe="")
        fake = (
            f"postgresql://{uq}:{pq}@{cfg['host']}:{int(cfg['port'])}/{cfg['dbname']}"
            f"?sslmode={quote(cfg['sslmode'], safe='')}"
        )
        pool_dsn = _supabase_direct_to_pooler_dsn(_normalize_database_url(fake))
        if pool_dsn != fake:
            return psycopg2.connect(pool_dsn, cursor_factory=RealDictCursor, connect_timeout=timeout)
    if _supabase_direct_db_host(cfg["host"]):
        v4 = _ipv4_hostaddr(cfg["host"])
        if v4:
            kw["hostaddr"] = v4
    return psycopg2.connect(**kw)


def init_db() -> tuple[bool, str]:
    ok, msg = db_ready()
    if not ok:
        return False, msg
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS hotel_case (
                    case_key TEXT PRIMARY KEY,
                    destination TEXT,
                    checkin DATE,
                    checkout DATE,
                    rooms INTEGER,
                    adults INTEGER,
                    children INTEGER,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS hotel_snapshot (
                    id BIGSERIAL PRIMARY KEY,
                    case_key TEXT NOT NULL REFERENCES hotel_case(case_key) ON DELETE CASCADE,
                    source TEXT NOT NULL,
                    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    destination TEXT,
                    hotel_name TEXT,
                    hotel_name_norm TEXT,
                    address TEXT,
                    address_norm TEXT,
                    star_text TEXT,
                    star_num DOUBLE PRECISION,
                    score_text TEXT,
                    price_vnd_raw TEXT,
                    price_vnd_num DOUBLE PRECISION,
                    cancel_policy TEXT,
                    meal_plan TEXT,
                    hotel_link TEXT,
                    raw_json JSONB
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_snapshot_case ON hotel_snapshot(case_key)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_snapshot_case_source ON hotel_snapshot(case_key, source)")
        conn.commit()
        return True, msg
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        hint = (
            " Streamlit Cloud: dán URI **Transaction pooler** (port 6543) từ Supabase → Database → Connect, "
            "hoặc giữ DATABASE_URL dạng db.*:5432 và thêm Secrets: SUPABASE_POOLER_REGION = \"ap-southeast-1\" "
            "(đúng region trong Project Settings → General / Database)."
        )
        err_head = str(e).strip().split("\n")[0][:220]
        return False, f"Không kết nối/khởi tạo DB: {type(e).__name__}: {err_head}{hint}"
    finally:
        if conn:
            conn.close()


def _to_ascii(text: str) -> str:
    txt = unicodedata.normalize("NFKD", str(text or ""))
    txt = "".join(ch for ch in txt if not unicodedata.combining(ch))
    txt = txt.replace("đ", "d").replace("Đ", "D")
    return txt.lower().strip()


def normalize_name(name: str, destination: str = "") -> str:
    txt = _to_ascii(name)
    txt = "".join(ch if ch.isalnum() or ch.isspace() else " " for ch in txt)
    tokens = [t for t in txt.split() if t and t not in STOPWORDS]
    dest_tokens = {t for t in _to_ascii(destination).split() if t}
    cleaned = [t for t in tokens if t not in dest_tokens and t not in CITY_WORDS]
    return " ".join(cleaned)


def normalize_address(address: str) -> str:
    txt = _to_ascii(address)
    txt = "".join(ch if ch.isalnum() or ch.isspace() else " " for ch in txt)
    return " ".join(tok for tok in txt.split() if tok)


def _parse_price_vnd(v: Any) -> float | None:
    if v is None:
        return None
    txt = str(v)
    digits = "".join(ch for ch in txt if ch.isdigit())
    if not digits:
        return None
    try:
        return float(digits)
    except Exception:
        return None


def _parse_star_num(v: Any) -> float | None:
    if v is None:
        return None
    txt = _to_ascii(str(v))
    num = ""
    for ch in txt:
        if ch.isdigit() or ch == ".":
            num += ch
        elif num:
            break
    if not num:
        return None
    try:
        return float(num)
    except Exception:
        return None


def replace_case_source(case_info: dict, source: str, rows: list[dict]) -> int:
    ok, msg = init_db()
    if not ok:
        raise RuntimeError(f"Supabase DB chưa sẵn sàng: {msg}")
    now = datetime.utcnow().isoformat(timespec="seconds")
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("BEGIN")
            cur.execute(
                """
                INSERT INTO hotel_case(case_key, destination, checkin, checkout, rooms, adults, children, created_at, updated_at)
                VALUES (%s, %s, NULLIF(%s,'')::date, NULLIF(%s,'')::date, %s, %s, %s, %s, %s)
                ON CONFLICT(case_key) DO UPDATE SET
                  destination=EXCLUDED.destination,
                  checkin=EXCLUDED.checkin,
                  checkout=EXCLUDED.checkout,
                  rooms=EXCLUDED.rooms,
                  adults=EXCLUDED.adults,
                  children=EXCLUDED.children,
                  updated_at=EXCLUDED.updated_at
                """,
                (
                    case_info["case_key"],
                    case_info.get("destination", ""),
                    case_info.get("checkin", ""),
                    case_info.get("checkout", ""),
                    int(case_info.get("rooms", 1)),
                    int(case_info.get("adults", 2)),
                    int(case_info.get("children", 0)),
                    now,
                    now,
                ),
            )

            # Replace rule: same case + same source => delete old rows, insert new rows.
            cur.execute(
                "DELETE FROM hotel_snapshot WHERE case_key = %s AND source = %s",
                (case_info["case_key"], source),
            )

            inserted = 0
            for r in rows or []:
                if not isinstance(r, dict):
                    continue
                cur.execute(
                    """
                    INSERT INTO hotel_snapshot(
                        case_key, source, captured_at, destination, hotel_name, hotel_name_norm,
                        address, address_norm, star_text, star_num, score_text, price_vnd_raw,
                        price_vnd_num, cancel_policy, meal_plan, hotel_link, raw_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        case_info["case_key"],
                        source,
                        now,
                        case_info.get("destination", ""),
                        r.get("Tên khách sạn", ""),
                        normalize_name(r.get("Tên khách sạn", ""), case_info.get("destination", "")),
                        r.get("Địa chỉ", ""),
                        normalize_address(r.get("Địa chỉ", "")),
                        r.get("Hạng sao", ""),
                        _parse_star_num(r.get("Hạng sao", "")),
                        r.get("Điểm đánh giá", ""),
                        r.get("Giá/đêm (VND)", ""),
                        _parse_price_vnd(r.get("Giá/đêm (VND)", "")),
                        r.get("Chính sách hoàn hủy", ""),
                        r.get("Gói bữa ăn", ""),
                        r.get("Link khách sạn", ""),
                        Json(r),
                    ),
                )
                inserted += 1
        conn.commit()
        return inserted
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_case_sources(case_key: str) -> list[str]:
    ok, _ = db_ready()
    if not ok:
        return []
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT source FROM hotel_snapshot WHERE case_key = %s ORDER BY source",
                (case_key,),
            )
            return [row["source"] for row in cur.fetchall()]
    finally:
        conn.close()


def list_hotel_cases(limit: int = 200, source: str | None = None) -> list[dict]:
    ok, _ = db_ready()
    if not ok:
        return []
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if source:
                cur.execute(
                    """
                    SELECT c.case_key, c.destination, c.checkin, c.checkout, c.rooms, c.adults, c.children,
                           c.updated_at,
                           COUNT(s.id) AS row_count,
                           COUNT(DISTINCT s.source) AS source_count
                    FROM hotel_case c
                    JOIN hotel_snapshot s ON s.case_key = c.case_key
                    WHERE EXISTS (
                        SELECT 1 FROM hotel_snapshot z
                        WHERE z.case_key = c.case_key AND z.source = %s
                    )
                    GROUP BY c.case_key, c.destination, c.checkin, c.checkout, c.rooms, c.adults, c.children, c.updated_at
                    ORDER BY c.updated_at DESC
                    LIMIT %s
                    """,
                    (source, int(limit)),
                )
            else:
                cur.execute(
                    """
                    SELECT c.case_key, c.destination, c.checkin, c.checkout, c.rooms, c.adults, c.children,
                           c.updated_at,
                           COUNT(s.id) AS row_count,
                           COUNT(DISTINCT s.source) AS source_count
                    FROM hotel_case c
                    LEFT JOIN hotel_snapshot s ON s.case_key = c.case_key
                    GROUP BY c.case_key, c.destination, c.checkin, c.checkout, c.rooms, c.adults, c.children, c.updated_at
                    ORDER BY c.updated_at DESC
                    LIMIT %s
                    """,
                    (int(limit),),
                )
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_hotels_by_case(case_key: str) -> list[dict]:
    ok, _ = db_ready()
    if not ok:
        return []
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT case_key, source, destination, hotel_name, hotel_name_norm, address, address_norm,
                       star_num, score_text, price_vnd_num, hotel_link
                FROM hotel_snapshot
                WHERE case_key = %s
                """,
                (case_key,),
            )
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_case_rows(case_key: str, source: str) -> list[dict]:
    ok, _ = db_ready()
    if not ok:
        return []
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT raw_json
                FROM hotel_snapshot
                WHERE case_key = %s AND source = %s
                ORDER BY id ASC
                """,
                (case_key, source),
            )
            out: list[dict] = []
            for row in cur.fetchall():
                raw = row.get("raw_json")
                if isinstance(raw, dict):
                    out.append(raw)
                elif isinstance(raw, str) and raw.strip():
                    try:
                        out.append(json.loads(raw))
                    except Exception:
                        pass
            return out
    finally:
        conn.close()


def _sim(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def _star_bonus(a: float | None, b: float | None) -> float:
    if a is None or b is None:
        return 0.0
    diff = abs(a - b)
    if diff <= 0.1:
        return 0.08
    if diff <= 0.5:
        return 0.04
    if diff <= 1.0:
        return 0.01
    return -0.03


def pair_score(base: dict, cand: dict) -> float:
    # Agoda often has weak/empty address -> rely mainly on name.
    agoda_mode = base.get("source") == "Agoda" or cand.get("source") == "Agoda"
    name_sc = _sim(base.get("hotel_name_norm", ""), cand.get("hotel_name_norm", ""))
    addr_sc = _sim(base.get("address_norm", ""), cand.get("address_norm", ""))
    if agoda_mode:
        total = 0.90 * name_sc + 0.10 * max(addr_sc, 0.0)
    else:
        total = 0.75 * name_sc + 0.25 * addr_sc
    total += _star_bonus(base.get("star_num"), cand.get("star_num"))
    return max(0.0, min(1.0, total))


def build_cross_channel_compare(case_key: str) -> list[dict]:
    rows = get_hotels_by_case(case_key)
    if not rows:
        return []
    by_source: dict[str, list[dict]] = {}
    for r in rows:
        by_source.setdefault(r["source"], []).append(r)

    all_sources = ["Travel.com.vn", "Agoda", "Trip.com", "Mytour.vn", "iVIVU"]
    available = [s for s in all_sources if s in by_source]
    if len(available) < 2:
        return []

    base_source = "Travel.com.vn" if "Travel.com.vn" in by_source else available[0]
    base_rows = by_source[base_source]

    result = []
    for b in base_rows:
        out = {
            "Case key": case_key,
            "Hotel chuẩn": b.get("hotel_name", ""),
            "Nguồn chuẩn": base_source,
            "Link chuẩn": b.get("hotel_link", ""),
        }
        base_price = b.get("price_vnd_num")
        out[f"Giá {base_source}"] = int(base_price) if isinstance(base_price, (int, float)) else ""

        prices = []
        if isinstance(base_price, (int, float)) and base_price > 0:
            prices.append((base_source, float(base_price)))

        for src in available:
            if src == base_source:
                continue
            best = None
            best_sc = -1.0
            for c in by_source.get(src, []):
                sc = pair_score(b, c)
                if sc > best_sc:
                    best_sc = sc
                    best = c
            match_name = best.get("hotel_name", "") if best else ""
            match_price = best.get("price_vnd_num") if best else None
            out[f"Match {src}"] = match_name
            out[f"Score {src}"] = f"{best_sc*100:.0f}" if best and best_sc >= 0 else ""
            out[f"Giá {src}"] = int(match_price) if isinstance(match_price, (int, float)) else ""

            if isinstance(match_price, (int, float)) and match_price > 0:
                prices.append((src, float(match_price)))
            if isinstance(base_price, (int, float)) and base_price > 0 and isinstance(match_price, (int, float)) and match_price > 0:
                gap_pct = ((match_price - base_price) / base_price) * 100.0
                out[f"Chênh {src} vs {base_source} (%)"] = f"{gap_pct:+.1f}%"
            else:
                out[f"Chênh {src} vs {base_source} (%)"] = ""

        if prices:
            cheapest = min(prices, key=lambda x: x[1])
            out["Kênh rẻ nhất"] = cheapest[0]
            out["Giá rẻ nhất"] = int(cheapest[1])
        else:
            out["Kênh rẻ nhất"] = ""
            out["Giá rẻ nhất"] = ""

        result.append(out)

    return result

