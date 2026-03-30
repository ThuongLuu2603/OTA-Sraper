import json
import os
import re
import socket
import unicodedata
from datetime import datetime
from difflib import SequenceMatcher
from math import atan2, cos, radians, sin, sqrt
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


# Pooler host thường là aws-0-<region> hoặc aws-1-<region> (tùy project Supabase).
_POOLER_AWS_PREFIXES: tuple[str, ...] = ("aws-0", "aws-1")

_COMMON_POOLER_REGIONS: tuple[str, ...] = (
    "ap-southeast-1",
    "ap-southeast-2",
    "ap-northeast-1",
    "ap-northeast-2",
    "ap-south-1",
    "us-east-1",
    "us-west-1",
    "eu-central-1",
    "eu-west-1",
    "eu-west-2",
    "ca-central-1",
    "sa-east-1",
)


def _build_pooler_dsn_from_direct(
    dsn: str, region: str | None, pooler_host: str | None
) -> str | None:
    """Từ URI db.<ref>.supabase.co:5432 → pooler 6543. Cần region hoặc pooler_host."""
    if not region and not pooler_host:
        return None
    u = urlparse(_normalize_database_url(dsn))
    hn = (u.hostname or "").lower()
    m = re.match(r"^db\.([a-z0-9]+)\.supabase\.co$", hn)
    if not m:
        return None
    if (u.port or 5432) != 5432:
        return None
    ref = m.group(1)
    host_out = pooler_host or (f"aws-0-{region}.pooler.supabase.com" if region else "")
    if not host_out:
        return None
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


def _direct_supabase_pooler_candidates(dsn: str) -> list[str]:
    """
    Các URI pooler để thử (ưu tiên region/host trong Secrets, sau đó thử nhiều region).
    Dùng khi Streamlit Cloud không kết nối được db.*:5432 (IPv6).
    """
    secrets = _safe_streamlit_secrets()
    user_region = _secret_str(os.getenv("SUPABASE_POOLER_REGION") or secrets.get("SUPABASE_POOLER_REGION", ""))
    user_host = _secret_str(os.getenv("SUPABASE_POOLER_HOST") or secrets.get("SUPABASE_POOLER_HOST", ""))
    out: list[str] = []
    seen: set[str] = set()

    def push(region: str | None, host: str | None) -> None:
        b = _build_pooler_dsn_from_direct(dsn, region, host)
        if b and b not in seen:
            seen.add(b)
            out.append(b)

    def push_region(r: str) -> None:
        for prefix in _POOLER_AWS_PREFIXES:
            push(r, f"{prefix}-{r}.pooler.supabase.com")

    if user_host:
        push(user_region or None, user_host)
        return out
    if user_region:
        push_region(user_region)
    for r in _COMMON_POOLER_REGIONS:
        if r == user_region:
            continue
        push_region(r)
    return out


def _supabase_direct_to_pooler_dsn(dsn: str) -> str:
    """
    Giữ tương thích: nếu có SUPABASE_POOLER_* trong Secrets thì đổi 1 lần.
    (Luồng chính dùng _direct_supabase_pooler_candidates + thử lần lượt.)
    """
    secrets = _safe_streamlit_secrets()
    region = _secret_str(os.getenv("SUPABASE_POOLER_REGION") or secrets.get("SUPABASE_POOLER_REGION", ""))
    pooler_host = _secret_str(os.getenv("SUPABASE_POOLER_HOST") or secrets.get("SUPABASE_POOLER_HOST", ""))
    built = _build_pooler_dsn_from_direct(dsn, region or None, pooler_host or None)
    return built if built else dsn


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
        u = urlparse(dsn)
        per_try_timeout = min(timeout, 12)
        if _supabase_direct_db_host(u.hostname or "") and (u.port or 5432) == 5432:
            for cand in _direct_supabase_pooler_candidates(dsn):
                try:
                    return psycopg2.connect(
                        cand, cursor_factory=RealDictCursor, connect_timeout=per_try_timeout
                    )
                except psycopg2.OperationalError:
                    continue
            kw_url = _connect_kw_from_database_url_ipv4(dsn, timeout)
            if kw_url:
                try:
                    return psycopg2.connect(**kw_url)
                except psycopg2.OperationalError:
                    pass
            return psycopg2.connect(dsn, cursor_factory=RealDictCursor, connect_timeout=timeout)
        dsn = _supabase_direct_to_pooler_dsn(dsn)
        kw_url = _connect_kw_from_database_url_ipv4(dsn, timeout)
        if kw_url:
            try:
                return psycopg2.connect(**kw_url)
            except psycopg2.OperationalError:
                pass
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
    if _supabase_direct_db_host(cfg["host"]) and int(cfg["port"]) == 5432:
        uq = quote(cfg["user"], safe="")
        pq = quote(cfg["password"], safe="")
        fake = (
            f"postgresql://{uq}:{pq}@{cfg['host']}:{int(cfg['port'])}/{cfg['dbname']}"
            f"?sslmode={quote(cfg['sslmode'], safe='')}"
        )
        per_try = min(timeout, 12)
        for cand in _direct_supabase_pooler_candidates(_normalize_database_url(fake)):
            try:
                return psycopg2.connect(
                    cand, cursor_factory=RealDictCursor, connect_timeout=per_try
                )
            except psycopg2.OperationalError:
                continue
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
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS tour_case (
                    case_key TEXT PRIMARY KEY,
                    destination TEXT,
                    period_start DATE,
                    period_end DATE,
                    currency TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS tour_snapshot (
                    id BIGSERIAL PRIMARY KEY,
                    case_key TEXT NOT NULL REFERENCES tour_case(case_key) ON DELETE CASCADE,
                    source TEXT NOT NULL,
                    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    destination TEXT,
                    tour_name TEXT,
                    tour_name_norm TEXT,
                    tour_code TEXT,
                    price_text TEXT,
                    currency TEXT,
                    depart_city TEXT,
                    raw_json JSONB
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tour_snapshot_case ON tour_snapshot(case_key)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tour_snapshot_case_source ON tour_snapshot(case_key, source)")
        conn.commit()
        return True, msg
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        hint = (
            " Nếu vẫn dùng db.*.supabase.co:5432: app đã thử pooler theo nhiều region; nếu hết cả — "
            "vào Supabase → Database → Connect → **Transaction pooler** và dán đúng URI (host pooler, port 6543, user postgres.<ref>)."
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


def replace_tour_case_source(case_info: dict, source: str, rows: list[dict]) -> int:
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
                INSERT INTO tour_case(case_key, destination, period_start, period_end, currency, created_at, updated_at)
                VALUES (%s, %s, NULLIF(%s,'')::date, NULLIF(%s,'')::date, NULLIF(%s,''), %s, %s)
                ON CONFLICT(case_key) DO UPDATE SET
                  destination=EXCLUDED.destination,
                  period_start=EXCLUDED.period_start,
                  period_end=EXCLUDED.period_end,
                  currency=EXCLUDED.currency,
                  updated_at=EXCLUDED.updated_at
                """,
                (
                    case_info["case_key"],
                    case_info.get("destination", ""),
                    case_info.get("period_start", ""),
                    case_info.get("period_end", ""),
                    case_info.get("currency", ""),
                    now,
                    now,
                ),
            )
            cur.execute(
                "DELETE FROM tour_snapshot WHERE case_key = %s AND source = %s",
                (case_info["case_key"], source),
            )
            inserted = 0
            dest = case_info.get("destination", "")
            for r in rows or []:
                if not isinstance(r, dict):
                    continue
                name = r.get("Tên tour", "") or ""
                cur.execute(
                    """
                    INSERT INTO tour_snapshot(
                        case_key, source, captured_at, destination, tour_name, tour_name_norm,
                        tour_code, price_text, currency, depart_city, raw_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        case_info["case_key"],
                        source,
                        now,
                        dest,
                        name,
                        normalize_name(name, dest),
                        r.get("Mã tour", ""),
                        r.get("Giá từ", ""),
                        r.get("Tiền tệ", ""),
                        r.get("Điểm khởi hành", ""),
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


def list_tour_cases(limit: int = 200, source: str | None = None) -> list[dict]:
    ok, _ = db_ready()
    if not ok:
        return []
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if source:
                cur.execute(
                    """
                    SELECT c.case_key, c.destination, c.period_start, c.period_end, c.currency, c.updated_at,
                           COUNT(s.id) AS row_count,
                           COUNT(DISTINCT s.source) AS source_count
                    FROM tour_case c
                    JOIN tour_snapshot s ON s.case_key = c.case_key
                    WHERE EXISTS (
                        SELECT 1 FROM tour_snapshot z
                        WHERE z.case_key = c.case_key AND z.source = %s
                    )
                    GROUP BY c.case_key, c.destination, c.period_start, c.period_end, c.currency, c.updated_at
                    ORDER BY c.updated_at DESC
                    LIMIT %s
                    """,
                    (source, int(limit)),
                )
            else:
                cur.execute(
                    """
                    SELECT c.case_key, c.destination, c.period_start, c.period_end, c.currency, c.updated_at,
                           COUNT(s.id) AS row_count,
                           COUNT(DISTINCT s.source) AS source_count
                    FROM tour_case c
                    LEFT JOIN tour_snapshot s ON s.case_key = c.case_key
                    GROUP BY c.case_key, c.destination, c.period_start, c.period_end, c.currency, c.updated_at
                    ORDER BY c.updated_at DESC
                    LIMIT %s
                    """,
                    (int(limit),),
                )
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def delete_tour_case(case_key: str) -> tuple[bool, str]:
    """Xóa case tour và toàn bộ snapshot (CASCADE)."""
    ok, msg = init_db()
    if not ok:
        return False, f"DB chưa sẵn sàng: {msg}"
    ck = (case_key or "").strip()
    if not ck:
        return False, "Thiếu case_key"
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM tour_case WHERE case_key = %s", (ck,))
            n = cur.rowcount
        conn.commit()
        if n == 0:
            return False, "Không tìm thấy case tour trong DB."
        return True, "Đã xóa case tour và mọi dữ liệu liên quan."
    except Exception as e:
        conn.rollback()
        return False, f"Lỗi xóa: {type(e).__name__}: {str(e)[:220]}"
    finally:
        conn.close()


def delete_tour_case_source(case_key: str, source: str) -> tuple[bool, str]:
    """Xóa snapshot của một nguồn; nếu case không còn dòng nào thì xóa luôn tour_case."""
    ok, msg = init_db()
    if not ok:
        return False, f"DB chưa sẵn sàng: {msg}"
    ck = (case_key or "").strip()
    src = (source or "").strip()
    if not ck or not src:
        return False, "Thiếu case_key hoặc source"
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM tour_snapshot WHERE case_key = %s AND source = %s",
                (ck, src),
            )
            removed = cur.rowcount
            cur.execute(
                "SELECT 1 FROM tour_snapshot WHERE case_key = %s LIMIT 1",
                (ck,),
            )
            if not cur.fetchone():
                cur.execute("DELETE FROM tour_case WHERE case_key = %s", (ck,))
        conn.commit()
        if removed == 0:
            return False, f"Không có dữ liệu {src} cho case này."
        return True, f"Đã xóa {removed} dòng tour của {src}."
    except Exception as e:
        conn.rollback()
        return False, f"Lỗi xóa: {type(e).__name__}: {str(e)[:220]}"
    finally:
        conn.close()


def get_tour_case_rows(case_key: str, source: str) -> list[dict]:
    ok, _ = db_ready()
    if not ok:
        return []
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT raw_json
                FROM tour_snapshot
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


def delete_hotel_case(case_key: str) -> tuple[bool, str]:
    """Xóa case khách sạn và toàn bộ snapshot mọi kênh (CASCADE)."""
    ok, msg = init_db()
    if not ok:
        return False, f"DB chưa sẵn sàng: {msg}"
    ck = (case_key or "").strip()
    if not ck:
        return False, "Thiếu case_key"
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM hotel_case WHERE case_key = %s", (ck,))
            n = cur.rowcount
        conn.commit()
        if n == 0:
            return False, "Không tìm thấy case trong DB."
        return True, "Đã xóa case và mọi dữ liệu khách sạn đã lưu (tất cả OTA)."
    except Exception as e:
        conn.rollback()
        return False, f"Lỗi xóa: {type(e).__name__}: {str(e)[:220]}"
    finally:
        conn.close()


def delete_hotel_case_source(case_key: str, source: str) -> tuple[bool, str]:
    """Xóa snapshot của một OTA; nếu case không còn dòng nào thì xóa luôn hotel_case."""
    ok, msg = init_db()
    if not ok:
        return False, f"DB chưa sẵn sàng: {msg}"
    ck = (case_key or "").strip()
    src = (source or "").strip()
    if not ck or not src:
        return False, "Thiếu case_key hoặc source"
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM hotel_snapshot WHERE case_key = %s AND source = %s",
                (ck, src),
            )
            removed = cur.rowcount
            cur.execute(
                "SELECT 1 FROM hotel_snapshot WHERE case_key = %s LIMIT 1",
                (ck,),
            )
            if not cur.fetchone():
                cur.execute("DELETE FROM hotel_case WHERE case_key = %s", (ck,))
        conn.commit()
        if removed == 0:
            return False, f"Không có dữ liệu {src} cho case này."
        return True, f"Đã xóa {removed} khách sạn đã lưu của {src}."
    except Exception as e:
        conn.rollback()
        return False, f"Lỗi xóa: {type(e).__name__}: {str(e)[:220]}"
    finally:
        conn.close()


def _raw_json_dict(raw: Any) -> dict:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            o = json.loads(raw)
            return o if isinstance(o, dict) else {}
        except Exception:
            return {}
    return {}


def _norm_property_id(val: Any) -> str:
    """Chuẩn hóa mã property (số Agoda, ID Travel, v.v.) để so khớp giữa kênh."""
    if val is None:
        return ""
    s = str(val).strip()
    if not s:
        return ""
    m = re.search(r"(\d{4,})", s)
    if m:
        return m.group(1)
    try:
        f = float(s.replace(",", "."))
        if f > 0 and f == int(f):
            return str(int(f))
    except ValueError:
        pass
    return s


def _parse_geo_coord(val: Any) -> float | None:
    if val is None:
        return None
    s = str(val).strip().replace(",", ".")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _coords_valid(lat: float | None, lng: float | None) -> bool:
    if lat is None or lng is None:
        return False
    return -90.0 <= lat <= 90.0 and -180.0 <= lng <= 180.0


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Khoảng cách địa lý (mét), WGS84."""
    r_earth = 6371000.0
    p1, p2 = radians(lat1), radians(lat2)
    dp = radians(lat2 - lat1)
    dl = radians(lon2 - lon1)
    a = sin(dp / 2) ** 2 + cos(p1) * cos(p2) * sin(dl / 2) ** 2
    return 2 * r_earth * atan2(sqrt(a), sqrt(max(0.0, 1.0 - a)))


def _enrich_snapshot_row_for_compare(row: dict) -> dict:
    """
    Gắn cmp_* từ raw_json (bản ghi scrape đầy đủ) để so sánh đa kênh.
    Không giữ raw_json trong dict trả về (tránh nặng / lặp).
    """
    out = dict(row)
    raw = _raw_json_dict(out.pop("raw_json", None))
    out["cmp_agoda_id"] = _norm_property_id(raw.get("Mã Property Agoda"))
    out["cmp_travel_id"] = _norm_property_id(raw.get("ID khách sạn Travel"))
    out["cmp_ota_id"] = _norm_property_id(raw.get("Mã property (OTA)"))
    out["cmp_lat"] = _parse_geo_coord(raw.get("Vĩ độ"))
    out["cmp_lng"] = _parse_geo_coord(raw.get("Kinh độ"))
    return out


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
                       star_num, score_text, price_vnd_num, hotel_link, raw_json
                FROM hotel_snapshot
                WHERE case_key = %s
                """,
                (case_key,),
            )
            return [_enrich_snapshot_row_for_compare(dict(row)) for row in cur.fetchall()]
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


# Điểm dưới ngưỡng → không coi là match (tránh ghép sai như Paris ↔ HERO Hostel).
MIN_CROSS_CHANNEL_MATCH_SCORE = 0.62

# So khớp theo tọa độ khi cả hai kênh (không dùng cặp ID Agoda–Travel) đều có lat/lng.
GEO_MATCH_MAX_M = 50.0
SRC_AGODA = "Agoda"
SRC_TRAVEL = "Travel.com.vn"
ID_AGODA_TRAVEL_MATCH_SCORE = 1.0


def _name_pair_score(na: str, nb: str) -> float:
    """
    Kết hợp SequenceMatcher + Jaccard trên token (tên đã normalize),
    bonus khi tập token của một bên là con của bên kia (vd. 'phi mai' ⊂ 'phi mai an giang').
    """
    na = (na or "").strip()
    nb = (nb or "").strip()
    if not na or not nb:
        return 0.0
    seq = SequenceMatcher(None, na, nb).ratio()
    t1 = {x for x in na.split() if x}
    t2 = {x for x in nb.split() if x}
    if not t1 or not t2:
        return seq
    inter = t1 & t2
    union = t1 | t2
    jacc = len(inter) / len(union) if union else 0.0
    core = 0.48 * seq + 0.52 * jacc
    if t1 <= t2 or t2 <= t1:
        core = min(1.0, core + 0.14)
    elif len(inter) >= 2 and inter:
        sm = min(len(t1), len(t2))
        if sm > 0 and len(inter) / sm >= 0.67:
            core = min(1.0, core + 0.07)
    return min(1.0, core)


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
    name_sc = _name_pair_score(base.get("hotel_name_norm", ""), cand.get("hotel_name_norm", ""))
    addr_sc = _sim(base.get("address_norm", ""), cand.get("address_norm", ""))
    if agoda_mode:
        total = 0.90 * name_sc + 0.10 * max(addr_sc, 0.0)
    else:
        total = 0.75 * name_sc + 0.25 * addr_sc
    total += _star_bonus(base.get("star_num"), cand.get("star_num"))
    return max(0.0, min(1.0, total))


def cross_pair_score_and_tag(base: dict, cand: dict) -> tuple[float, str]:
    """
    Ghép đa kênh:
    - Agoda ↔ Travel.com.vn: cùng Mã Property Agoda (Travel lưu ID Agoda liên kết ở cột đó).
    - Các cặp khác: nếu cả hai có GEO → chỉ xét cặp trong sai số GEO_MATCH_MAX_M (mặc định 50m),
      điểm ghép chủ yếu theo tên (+ sao nhẹ).
    - Thiếu tọa độ một/bên: giữ logic cũ (tên + địa chỉ).
    """
    sa = (base.get("source") or "").strip()
    sb = (cand.get("source") or "").strip()

    if {sa, sb} == {SRC_AGODA, SRC_TRAVEL}:
        row_agoda = base if sa == SRC_AGODA else cand
        row_travel = cand if sa == SRC_AGODA else base
        id_agoda = (row_agoda.get("cmp_agoda_id") or "").strip()
        id_on_travel = (row_travel.get("cmp_agoda_id") or "").strip()
        if id_agoda and id_on_travel and id_agoda == id_on_travel:
            return (ID_AGODA_TRAVEL_MATCH_SCORE, "ID Agoda–Travel")

    la1, lo1 = base.get("cmp_lat"), base.get("cmp_lng")
    la2, lo2 = cand.get("cmp_lat"), cand.get("cmp_lng")
    if _coords_valid(la1, lo1) and _coords_valid(la2, lo2):
        dist = _haversine_m(la1, lo1, la2, lo2)
        if dist > GEO_MATCH_MAX_M:
            return (0.0, f">{GEO_MATCH_MAX_M:.0f}m")
        name_sc = _name_pair_score(
            base.get("hotel_name_norm", ""), cand.get("hotel_name_norm", "")
        )
        addr_sc = _sim(base.get("address_norm", ""), cand.get("address_norm", ""))
        total = 0.92 * name_sc + 0.08 * max(addr_sc, 0.0)
        total += _star_bonus(base.get("star_num"), cand.get("star_num"))
        total = max(0.0, min(1.0, total))
        return (total, f"GEO≤{GEO_MATCH_MAX_M:.0f}m+tên")

    sc = pair_score(base, cand)
    return (sc, "Tên+địa chỉ")


def _greedy_one_to_one(
    base_rows: list[dict],
    cand_rows: list[dict],
    min_score: float,
) -> tuple[dict[int, tuple[dict, float, str]], set[int]]:
    """
    Mỗi khách sạn phía candidate chỉ gán cho tối đa một dòng base (tránh iVIVU bị lặp cho nhiều Agoda).
    Duyệt cạnh theo điểm giảm dần, bỏ qua nếu đã khớp.
    """
    edges: list[tuple[float, int, int, str]] = []
    for i, b in enumerate(base_rows):
        for j, c in enumerate(cand_rows):
            sc, tag = cross_pair_score_and_tag(b, c)
            edges.append((sc, i, j, tag))
    edges.sort(key=lambda x: x[0], reverse=True)
    used_b: set[int] = set()
    used_c: set[int] = set()
    out: dict[int, tuple[dict, float, str]] = {}
    for sc, i, j, tag in edges:
        if sc < min_score:
            break
        if i in used_b or j in used_c:
            continue
        used_b.add(i)
        used_c.add(j)
        out[i] = (cand_rows[j], sc, tag)
    return out, used_c


def _pick_base_source(available: list[str], by_source: dict[str, list]) -> str:
    """Nguồn có nhiều bản ghi nhất làm trục so sánh; hòa thì ưu tiên thứ tự (tránh mặc định Agoda)."""
    best_n = max(len(by_source[s]) for s in available)
    cand = [s for s in available if len(by_source[s]) == best_n]
    tie_pref = ["Travel.com.vn", "Trip.com", "Mytour.vn", "iVIVU", "Agoda"]
    for o in tie_pref:
        if o in cand:
            return o
    return cand[0]


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

    base_source = _pick_base_source(available, by_source)
    base_rows = by_source[base_source]

    # Gán 1–1 giữa base và từng nguồn khác (mỗi KS phía đối chỉ dùng một lần).
    assign_per_src: dict[str, dict[int, tuple[dict, float, str]]] = {}
    used_cand_per_src: dict[str, set[int]] = {}
    for src in available:
        if src == base_source:
            continue
        cands = by_source.get(src, [])
        if not cands:
            assign_per_src[src] = {}
            used_cand_per_src[src] = set()
        else:
            assign, used_j = _greedy_one_to_one(
                base_rows, cands, MIN_CROSS_CHANNEL_MATCH_SCORE
            )
            assign_per_src[src] = assign
            used_cand_per_src[src] = used_j

    result: list[dict] = []
    for idx, b in enumerate(base_rows):
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
            match_tag = ""
            pair = assign_per_src.get(src, {}).get(idx)
            if pair:
                best, best_sc, match_tag = pair
            match_name = best.get("hotel_name", "") if best else ""
            match_price = best.get("price_vnd_num") if best else None
            out[f"Match {src}"] = match_name
            out[f"Score {src}"] = f"{best_sc * 100:.0f}" if best and best_sc >= MIN_CROSS_CHANNEL_MATCH_SCORE else ""
            out[f"Cách ghép {src}"] = match_tag if best else ""
            out[f"Giá {src}"] = int(match_price) if isinstance(match_price, (int, float)) else ""

            if isinstance(match_price, (int, float)) and match_price > 0:
                prices.append((src, float(match_price)))
            if (
                isinstance(base_price, (int, float))
                and base_price > 0
                and isinstance(match_price, (int, float))
                and match_price > 0
                and best
            ):
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

        note_parts = []
        for src in available:
            if src == base_source:
                continue
            if not assign_per_src.get(src, {}).get(idx):
                note_parts.append(f"không khớp {src}")
        out["Ghi chú"] = "; ".join(note_parts) if note_parts else ""

        result.append(out)

    # Khách sạn chỉ có trên kênh khác (không được ghép 1–1 với base) — vẫn hiện một dòng riêng.
    for src in available:
        if src == base_source:
            continue
        cands = by_source.get(src, [])
        used_j = used_cand_per_src.get(src, set())
        for j, c in enumerate(cands):
            if j in used_j:
                continue
            out = {
                "Case key": case_key,
                "Hotel chuẩn": c.get("hotel_name", ""),
                "Nguồn chuẩn": src,
                "Link chuẩn": c.get("hotel_link", ""),
            }
            prices = []
            c_price = c.get("price_vnd_num")
            for s in available:
                out[f"Giá {s}"] = ""
                if s != base_source:
                    out[f"Match {s}"] = ""
                    out[f"Score {s}"] = ""
                    out[f"Cách ghép {s}"] = ""
                    out[f"Chênh {s} vs {base_source} (%)"] = ""
            out[f"Giá {src}"] = int(c_price) if isinstance(c_price, (int, float)) else ""
            out[f"Match {src}"] = c.get("hotel_name", "")
            out[f"Score {src}"] = ""
            out[f"Chênh {src} vs {base_source} (%)"] = ""
            if isinstance(c_price, (int, float)) and c_price > 0:
                prices.append((src, float(c_price)))
            if prices:
                cheapest = min(prices, key=lambda x: x[1])
                out["Kênh rẻ nhất"] = cheapest[0]
                out["Giá rẻ nhất"] = int(cheapest[1])
            else:
                out["Kênh rẻ nhất"] = ""
                out["Giá rẻ nhất"] = ""
            out["Ghi chú"] = (
                f"Chỉ có trên {src} (chưa ghép với {base_source}: xa >{GEO_MATCH_MAX_M:.0f}m, "
                f"không khớp ID Agoda–Travel, hoặc điểm tên < {MIN_CROSS_CHANNEL_MATCH_SCORE:.0%})"
            )
            result.append(out)

    return result

