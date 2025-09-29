#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Zendesk → MySQL incremental backup
- Users (cursor)
- Organizations (time-based incremental, throttled)
- Tickets (cursor) + per-ticket Comments + Attachments
- Views, Triggers, Trigger Categories, Macros (snapshot lists)

Enhancements:
- Downloads attachment binaries locally and stores path in attachments.local_path
- Env toggles for attachment downloading, throttling, and skipping parts
"""

# ---- Minimal friendly dependency bootstrap -----------------------------------
def _need(pkg: str, pip_name: str):
    try:
        return __import__(pkg)
    except Exception:
        raise SystemExit(
            f"Missing dependency '{pip_name}'.\n"
            f"Activate your venv and install requirements:\n"
            f"  source .venv/bin/activate\n"
            f"  pip install -r requirements.txt\n"
            f"Or install just this package:\n"
            f"  pip install {pip_name}\n"
        )

requests = _need("requests", "requests")
mysql_connector_pkg = _need("mysql", "mysql-connector-python")
dotenv_pkg = _need("dotenv", "python-dotenv")

# ------------------------------------------------------------------------------
import os, time, json, logging, re
import datetime as dt
from pathlib import Path
from typing import Dict, Any, Iterable, Optional, Union

import mysql.connector  # provided by mysql-connector-python
from dotenv import load_dotenv

load_dotenv()

# ----------------------------
# Config (from .env)
# ----------------------------
Z_SUBDOMAIN = os.getenv("ZENDESK_SUBDOMAIN")
Z_EMAIL = os.getenv("ZENDESK_EMAIL")
Z_TOKEN = os.getenv("ZENDESK_API_TOKEN")
assert Z_SUBDOMAIN and Z_EMAIL and Z_TOKEN, "Zendesk credentials missing in .env"

MYSQL_CFG = {
    "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "database": os.getenv("MYSQL_DB", "zendesk_backup"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASS"),
    "autocommit": True,
}
assert MYSQL_CFG["user"] and MYSQL_CFG["password"], "MySQL creds missing in .env"

Z_BASE = f"https://{Z_SUBDOMAIN}.zendesk.com"
PER_PAGE = int(os.getenv("ZENDESK_PER_PAGE", "500"))
INCLUDE = os.getenv("ZENDESK_INCLUDE", "")
EXCLUDE_DELETED = os.getenv("ZENDESK_EXCLUDE_DELETED", "false").lower() == "true"
BOOTSTRAP_HOURS = int(os.getenv("ZENDESK_BOOTSTRAP_START_HOURS", "24"))

# Tickets/Comments toggles
CLOSED_TICKETS_ONLY = os.getenv("CLOSED_TICKETS_ONLY", "false").lower() == "true"
USE_TICKET_EVENTS_FOR_COMMENTS = os.getenv("USE_TICKET_EVENTS_FOR_COMMENTS", "false").lower() == "true"
PRUNE_REOPENED_FROM_DB = os.getenv("PRUNE_REOPENED_FROM_DB", "false").lower() == "true"

# Attachments
DOWNLOAD_ATTACHMENTS = os.getenv("DOWNLOAD_ATTACHMENTS", "true").lower() == "true"
ATTACHMENTS_DIR = os.getenv("ATTACHMENTS_DIR", "./attachments")

# Orgs throttling / skipping
ORG_PER_PAGE = int(os.getenv("ORG_PER_PAGE", "100"))
ORG_PAGE_DELAY_SECS = float(os.getenv("ORG_PAGE_DELAY_SECS", "1.5"))
SKIP_ORGANIZATIONS = os.getenv("SKIP_ORGANIZATIONS", "false").lower() == "true"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

SESSION = requests.Session()
SESSION.auth = (f"{Z_EMAIL}/token", Z_TOKEN)
SESSION.headers.update({
    "Accept": "application/json",
    "User-Agent": "zendesk-backup/1.0 (+python-requests)"
})
RETRY_STATUS = {429, 500, 502, 503, 504}

# ----------------------------
# Helpers
# ----------------------------
def ensure_dir(p: str) -> None:
    Path(p).mkdir(parents=True, exist_ok=True)

def safe_filename(name: str) -> str:
    name = re.sub(r"[\\/:*?\"<>|]+", "_", name or "")
    return name[:180] if len(name) > 180 else name

def initial_start_time_epoch() -> int:
    # at least 2 minutes buffer per Zendesk docs
    return int(time.time()) - max(BOOTSTRAP_HOURS * 3600, 120)

def parse_dt(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def get_with_retry(url: str, params: Dict[str, Any] = None, stream: bool = False) -> Union[requests.Response, Dict[str, Any]]:
    backoff = 1.0
    for _ in range(8):
        resp = SESSION.get(url, params=params, timeout=120, stream=stream)
        if resp.status_code == 200:
            return resp if stream else resp.json()
        if resp.status_code in RETRY_STATUS:
            retry_after = resp.headers.get("Retry-After")
            sleep_for = float(retry_after) if retry_after else backoff
            logging.warning("Retryable %s for %s. Sleeping %.1fs", resp.status_code, url, sleep_for)
            time.sleep(sleep_for)
            backoff = min(backoff * 2, 30.0)
            continue
        try:
            detail = resp.json()
        except Exception:
            detail = {"text": resp.text[:300]}
        raise RuntimeError(f"GET {url} failed [{resp.status_code}]: {detail}")
    raise RuntimeError(f"GET {url} exhausted retries")

def iter_list_pages(url: str, params: Dict[str, Any] = None) -> Iterable[Dict[str, Any]]:
    page = get_with_retry(url, params=params or {})
    while True:
        yield page
        next_url = page.get("next_page") or page.get("links", {}).get("next")
        if not next_url:
            break
        page = get_with_retry(next_url)

def iter_cursor_page(first_page_json: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    page = first_page_json
    while True:
        yield page
        after_url = page.get("after_url") or page.get("links", {}).get("next")
        if not after_url:
            break
        page = get_with_retry(after_url)

# ----------------------------
# DB
# ----------------------------
def get_db():
    return mysql.connector.connect(**MYSQL_CFG)

SCHEMA_SQL = """
CREATE DATABASE IF NOT EXISTS {db} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE {db};

CREATE TABLE IF NOT EXISTS sync_state (
  resource VARCHAR(64) PRIMARY KEY,
  cursor_token VARCHAR(255) NOT NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_snapshots (
  resource VARCHAR(64) NOT NULL,
  entity_id BIGINT NOT NULL,
  updated_at DATETIME NULL,
  payload_json JSON NOT NULL,
  PRIMARY KEY (resource, entity_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS users (
  id BIGINT PRIMARY KEY,
  name VARCHAR(255),
  email VARCHAR(255),
  role VARCHAR(32),
  role_type INT NULL,
  active TINYINT(1),
  suspended TINYINT(1),
  organization_id BIGINT NULL,
  phone VARCHAR(64) NULL,
  locale VARCHAR(32) NULL,
  time_zone VARCHAR(128) NULL,
  created_at DATETIME NULL,
  updated_at DATETIME NULL,
  last_login_at DATETIME NULL,
  tags_json JSON NULL,
  user_fields_json JSON NULL,
  photo_json JSON NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS organizations (
  id BIGINT PRIMARY KEY,
  name VARCHAR(255),
  external_id VARCHAR(255) NULL,
  group_id BIGINT NULL,
  details TEXT NULL,
  notes TEXT NULL,
  shared_tickets TINYINT(1) NULL,
  shared_comments TINYINT(1) NULL,
  domain_names_json JSON NULL,
  tags_json JSON NULL,
  organization_fields_json JSON NULL,
  created_at DATETIME NULL,
  updated_at DATETIME NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS tickets (
  id BIGINT PRIMARY KEY,
  subject VARCHAR(1024),
  description MEDIUMTEXT,
  status VARCHAR(32),
  priority VARCHAR(32),
  type VARCHAR(32),
  requester_id BIGINT,
  assignee_id BIGINT,
  organization_id BIGINT,
  created_at DATETIME,
  updated_at DATETIME,
  due_at DATETIME NULL,
  KEY idx_status (status),
  KEY idx_updated_at (updated_at)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS ticket_comments (
  id BIGINT PRIMARY KEY,
  ticket_id BIGINT NOT NULL,
  author_id BIGINT NULL,
  public TINYINT(1) NOT NULL,
  body MEDIUMTEXT,
  created_at DATETIME,
  updated_at DATETIME,
  KEY idx_ticket (ticket_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS attachments (
  id BIGINT PRIMARY KEY,
  ticket_id BIGINT,
  comment_id BIGINT,
  file_name VARCHAR(1024),
  content_url TEXT,
  local_path VARCHAR(1024) NULL,
  content_type VARCHAR(255),
  size BIGINT,
  thumbnails_json JSON NULL,
  created_at DATETIME,
  KEY idx_ticket (ticket_id),
  KEY idx_comment (comment_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS views (
  id BIGINT PRIMARY KEY,
  title VARCHAR(255),
  description TEXT NULL,
  active TINYINT(1),
  position INT NULL,
  default_view TINYINT(1) NULL,
  restriction_json JSON NULL,
  execution_json JSON NULL,
  conditions_json JSON NULL,
  created_at DATETIME NULL,
  updated_at DATETIME NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS triggers (
  id BIGINT PRIMARY KEY,
  title VARCHAR(255),
  description TEXT NULL,
  active TINYINT(1),
  position INT NULL,
  category_id VARCHAR(64) NULL,
  raw_title VARCHAR(255) NULL,
  default_trigger TINYINT(1) NULL,
  conditions_json JSON NOT NULL,
  actions_json JSON NOT NULL,
  created_at DATETIME NULL,
  updated_at DATETIME NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS trigger_categories (
  id VARCHAR(64) PRIMARY KEY,
  name VARCHAR(255),
  position INT NULL,
  created_at DATETIME NULL,
  updated_at DATETIME NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS macros (
  id BIGINT PRIMARY KEY,
  title VARCHAR(255),
  description TEXT NULL,
  active TINYINT(1),
  position INT NULL,
  default_macro TINYINT(1) NULL,
  restriction_json JSON NULL,
  actions_json JSON NOT NULL,
  created_at DATETIME NULL,
  updated_at DATETIME NULL
) ENGINE=InnoDB;
"""

def init_schema():
    conn = get_db()
    cur = conn.cursor()
    for stmt in [s for s in SCHEMA_SQL.format(db=MYSQL_CFG["database"]).split(";\n") if s.strip()]:
        cur.execute(stmt)
    conn.commit()
    cur.close()
    conn.close()

def get_cursor_val(conn, resource: str) -> Optional[str]:
    cur = conn.cursor()
    cur.execute("SELECT cursor_token FROM sync_state WHERE resource=%s", (resource,))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None

def set_cursor_val(conn, resource: str, token: str):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO sync_state (resource, cursor_token)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE cursor_token=VALUES(cursor_token), updated_at=CURRENT_TIMESTAMP
    """, (resource, token))
    conn.commit()
    cur.close()

def upsert_raw(conn, resource: str, entity_id: int, updated_at: Optional[str], payload: Dict[str, Any]):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO raw_snapshots (resource, entity_id, updated_at, payload_json)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE updated_at=VALUES(updated_at), payload_json=VALUES(payload_json)
    """, (resource, entity_id, parse_dt(updated_at), json.dumps(payload, ensure_ascii=False)))
    conn.commit()
    cur.close()

# ----------------------------
# UPSERT helpers
# ----------------------------
def upsert_user(conn, u: Dict[str, Any]):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO users (id, name, email, role, role_type, active, suspended, organization_id, phone, locale, time_zone,
                           created_at, updated_at, last_login_at, tags_json, user_fields_json, photo_json)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          name=VALUES(name), email=VALUES(email), role=VALUES(role), role_type=VALUES(role_type),
          active=VALUES(active), suspended=VALUES(suspended), organization_id=VALUES(organization_id),
          phone=VALUES(phone), locale=VALUES(locale), time_zone=VALUES(time_zone),
          created_at=VALUES(created_at), updated_at=VALUES(updated_at), last_login_at=VALUES(last_login_at),
          tags_json=VALUES(tags_json), user_fields_json=VALUES(user_fields_json), photo_json=VALUES(photo_json)
    """, (
        u.get("id"), u.get("name"), u.get("email"), u.get("role"), u.get("role_type"),
        1 if u.get("active") else 0, 1 if u.get("suspended") else 0, u.get("organization_id"),
        u.get("phone"), u.get("locale"), u.get("time_zone"),
        parse_dt(u.get("created_at")), parse_dt(u.get("updated_at")), parse_dt(u.get("last_login_at")),
        json.dumps(u.get("tags") or []), json.dumps(u.get("user_fields") or {}), json.dumps(u.get("photo") or {})
    ))
    conn.commit(); cur.close()
    upsert_raw(conn, "users", int(u["id"]), u.get("updated_at"), u)

def upsert_org(conn, o: Dict[str, Any]):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO organizations (id, name, external_id, group_id, details, notes, shared_tickets, shared_comments,
                                   domain_names_json, tags_json, organization_fields_json, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          name=VALUES(name), external_id=VALUES(external_id), group_id=VALUES(group_id),
          details=VALUES(details), notes=VALUES(notes), shared_tickets=VALUES(shared_tickets),
          shared_comments=VALUES(shared_comments), domain_names_json=VALUES(domain_names_json),
          tags_json=VALUES(tags_json), organization_fields_json=VALUES(organization_fields_json),
          created_at=VALUES(created_at), updated_at=VALUES(updated_at)
    """, (
        o.get("id"), o.get("name"), o.get("external_id"), o.get("group_id"), o.get("details"), o.get("notes"),
        1 if o.get("shared_tickets") else 0, 1 if o.get("shared_comments") else 0,
        json.dumps(o.get("domain_names") or []), json.dumps(o.get("tags") or []),
        json.dumps(o.get("organization_fields") or {}), parse_dt(o.get("created_at")), parse_dt(o.get("updated_at"))
    ))
    conn.commit(); cur.close()
    upsert_raw(conn, "organizations", int(o["id"]), o.get("updated_at"), o)

def upsert_ticket(conn, t: Dict[str, Any]):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO tickets (id, subject, description, status, priority, type, requester_id, assignee_id, organization_id,
                             created_at, updated_at, due_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          subject=VALUES(subject), description=VALUES(description), status=VALUES(status),
          priority=VALUES(priority), type=VALUES(type), requester_id=VALUES(requester_id),
          assignee_id=VALUES(assignee_id), organization_id=VALUES(organization_id),
          created_at=VALUES(created_at), updated_at=VALUES(updated_at), due_at=VALUES(due_at)
    """, (
        t.get("id"), t.get("subject"), t.get("description"), t.get("status"), t.get("priority"), t.get("type"),
        t.get("requester_id"), t.get("assignee_id"), t.get("organization_id"),
        parse_dt(t.get("created_at")), parse_dt(t.get("updated_at")), parse_dt(t.get("due_at"))
    ))
    conn.commit(); cur.close()
    upsert_raw(conn, "tickets", int(t["id"]), t.get("updated_at"), t)

def delete_ticket(conn, ticket_id: int):
    cur = conn.cursor()
    cur.execute("DELETE FROM attachments WHERE ticket_id=%s", (ticket_id,))
    cur.execute("DELETE FROM ticket_comments WHERE ticket_id=%s", (ticket_id,))
    cur.execute("DELETE FROM tickets WHERE id=%s", (ticket_id,))
    conn.commit()
    cur.close()

def upsert_comment(conn, ticket_id: int, c: Dict[str, Any]):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO ticket_comments (id, ticket_id, author_id, public, body, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          ticket_id=VALUES(ticket_id), author_id=VALUES(author_id), public=VALUES(public),
          body=VALUES(body), created_at=VALUES(created_at), updated_at=VALUES(updated_at)
    """, (
        c.get("id"), ticket_id, c.get("author_id"), 1 if c.get("public") else 0,
        c.get("body"), parse_dt(c.get("created_at")), parse_dt(c.get("updated_at") or c.get("created_at"))
    ))
    conn.commit(); cur.close()
    upsert_raw(conn, "comments", int(c["id"]), c.get("updated_at") or c.get("created_at"), c)

def upsert_attachment(conn, ticket_id: int, comment_id: Optional[int], a: Dict[str, Any], local_path: Optional[str] = None):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO attachments (id, ticket_id, comment_id, file_name, content_url, local_path, content_type, size, thumbnails_json, created_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          ticket_id=VALUES(ticket_id), comment_id=VALUES(comment_id), file_name=VALUES(file_name),
          content_url=VALUES(content_url), local_path=VALUES(local_path), content_type=VALUES(content_type),
          size=VALUES(size), thumbnails_json=VALUES(thumbnails_json), created_at=VALUES(created_at)
    """, (
        a.get("id"), ticket_id, comment_id, a.get("file_name"), a.get("content_url"),
        local_path, a.get("content_type"), a.get("size"), json.dumps(a.get("thumbnails") or []),
        parse_dt(a.get("created_at"))
    ))
    conn.commit(); cur.close()
    upsert_raw(conn, "attachments", int(a["id"]), a.get("created_at"), a)

# ----------------------------
# Downloads
# ----------------------------
def download_attachment(ticket_id: int, comment_id: Optional[int], att: Dict[str, Any]) -> Optional[str]:
    """
    Download the binary to ATTACHMENTS_DIR/<ticket_id>/<attachment_id>__<safe_filename>
    Return local file path or None if disabled/failure.
    """
    if not DOWNLOAD_ATTACHMENTS:
        return None
    url = att.get("content_url")
    if not url:
        return None

    base_dir = Path(ATTACHMENTS_DIR) / str(ticket_id)
    ensure_dir(str(base_dir))

    rid = str(att.get("id"))
    fname = safe_filename(att.get("file_name") or f"attachment_{rid}")
    target = base_dir / f"{rid}__{fname}"

    if target.exists() and target.stat().st_size > 0:
        return str(target.resolve())

    try:
        resp = get_with_retry(url, stream=True)  # same SESSION auth
        with open(target, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        return str(target.resolve())
    except Exception as e:
        logging.warning("Attachment download failed (ticket %s, att %s): %s", ticket_id, att.get("id"), e)
        return None

# ----------------------------
# Sync functions
# ----------------------------
def sync_users(conn):
    url = f"{Z_BASE}/api/v2/incremental/users/cursor.json"
    last = get_cursor_val(conn, "users")
    params = {"per_page": PER_PAGE}
    if last:
        params["cursor"] = last
    else:
        params["start_time"] = initial_start_time_epoch()

    first = get_with_retry(url, params=params)
    for page in iter_cursor_page(first):
        for u in page.get("users", []):
            upsert_user(conn, u)
        ac = page.get("after_cursor")
        if ac:
            set_cursor_val(conn, "users", ac)
        if page.get("end_of_stream"):
            break
    logging.info("Users: sync complete.")

def sync_organizations(conn):
    if SKIP_ORGANIZATIONS:
        logging.info("Organizations: skipped (SKIP_ORGANIZATIONS=true).")
        return

    url = f"{Z_BASE}/api/v2/incremental/organizations.json"
    last_epoch = get_cursor_val(conn, "organizations")
    params = {
        "per_page": ORG_PER_PAGE,
        "start_time": int(last_epoch) if last_epoch else initial_start_time_epoch()
    }

    while True:
        page = get_with_retry(url, params=params)
        for o in page.get("organizations", []):
            upsert_org(conn, o)

        end_time = page.get("end_time")
        if end_time:
            set_cursor_val(conn, "organizations", str(end_time))

        next_page = page.get("next_page")
        if not next_page:
            break

        time.sleep(ORG_PAGE_DELAY_SECS)  # throttle to avoid 429s
        url = next_page
        params = {}  # next_page is a full URL

    logging.info("Organizations: sync complete.")

def ticket_initial_params(last_cursor: Optional[str]) -> Dict[str, Any]:
    p = {"per_page": PER_PAGE}
    if INCLUDE:
        p["include"] = INCLUDE
    if EXCLUDE_DELETED:
        p["exclude_deleted"] = "true"
    if last_cursor:
        p["cursor"] = last_cursor
    else:
        p["start_time"] = initial_start_time_epoch()
    return p

def sync_tickets_comments_attachments(conn):
    url = f"{Z_BASE}/api/v2/incremental/tickets/cursor.json"
    last = get_cursor_val(conn, "tickets")
    first = get_with_retry(url, params=ticket_initial_params(last))

    for page in iter_cursor_page(first):
        for t in page.get("tickets", []):
            status = (t.get("status") or "").lower()
            is_closed = (status == "closed")

            if CLOSED_TICKETS_ONLY and not is_closed:
                if PRUNE_REOPENED_FROM_DB:
                    delete_ticket(conn, int(t["id"]))
                continue

            upsert_ticket(conn, t)

            if not USE_TICKET_EVENTS_FOR_COMMENTS:
                comments_url = f"{Z_BASE}/api/v2/tickets/{t['id']}/comments.json"
                for cpage in iter_list_pages(comments_url, params={"per_page": 100}):
                    for c in cpage.get("comments", []):
                        upsert_comment(conn, int(t["id"]), c)
                        for a in (c.get("attachments") or []):
                            local_path = download_attachment(int(t["id"]), int(c.get("id") or 0), a)
                            upsert_attachment(conn, int(t["id"]), int(c.get("id") or 0), a, local_path)

        ac = page.get("after_cursor")
        if ac:
            set_cursor_val(conn, "tickets", ac)
        if page.get("end_of_stream"):
            break
    logging.info("Tickets (+comments, attachments): sync complete (closed-only=%s, download=%s).",
                 CLOSED_TICKETS_ONLY, DOWNLOAD_ATTACHMENTS)

def sync_ticket_events_for_comments(conn):
    url = f"{Z_BASE}/api/v2/incremental/ticket_events.json"
    last_epoch = get_cursor_val(conn, "ticket_events")
    params = {"start_time": int(last_epoch) if last_epoch else initial_start_time_epoch(),
              "per_page": PER_PAGE, "include": "comment_events"}

    while True:
        page = get_with_retry(url, params=params)
        for ev in page.get("ticket_events", []):
            tid = int(ev.get("ticket_id"))
            if CLOSED_TICKETS_ONLY:
                t = get_with_retry(f"{Z_BASE}/api/v2/tickets/{tid}.json").get("ticket", {})
                if (t.get("status") or "").lower() != "closed":
                    continue
            for ce in (ev.get("child_events") or []):
                if (ce.get("event_type") or ce.get("type")) == "Comment":
                    c = {
                        "id": ce["id"],
                        "author_id": ce.get("author_id"),
                        "public": ce.get("public", False),
                        "body": ce.get("body"),
                        "created_at": ce.get("created_at"),
                        "updated_at": ce.get("created_at"),
                        "attachments": ce.get("attachments") or []
                    }
                    upsert_comment(conn, tid, c)
                    for a in (ce.get("attachments") or []):
                        local_path = download_attachment(tid, int(ce["id"]), a)
                        upsert_attachment(conn, tid, int(ce["id"]), a, local_path)
        next_page = page.get("next_page")
        if next_page:
            url = next_page
            params = {}
        else:
            end_time = page.get("end_time")
            if end_time:
                set_cursor_val(conn, "ticket_events", str(end_time))
            break
    logging.info("Ticket events (comments): sync complete.")

def sync_views(conn):
    url = f"{Z_BASE}/api/v2/views.json"
    params = {"per_page": min(PER_PAGE, 100)}
    if INCLUDE:
        params["include"] = INCLUDE
    page = get_with_retry(url, params=params)
    while True:
        for v in page.get("views", []):
            upsert_view(conn, v)
        next_page = page.get("next_page") or page.get("links", {}).get("next")
        if not next_page:
            break
        page = get_with_retry(next_page)
    logging.info("Views: sync complete.")

def upsert_view(conn, v: Dict[str, Any]):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO views (id, title, description, active, position, default_view, restriction_json, execution_json, conditions_json,
                           created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
           title=VALUES(title), description=VALUES(description), active=VALUES(active), position=VALUES(position),
           default_view=VALUES(default_view), restriction_json=VALUES(restriction_json),
           execution_json=VALUES(execution_json), conditions_json=VALUES(conditions_json),
           created_at=VALUES(created_at), updated_at=VALUES(updated_at)
    """, (
        v.get("id"), v.get("title"), v.get("description"), 1 if v.get("active") else 0, v.get("position"),
        1 if v.get("default") else 0, json.dumps(v.get("restriction")), json.dumps(v.get("execution")),
        json.dumps(v.get("conditions")), parse_dt(v.get("created_at")), parse_dt(v.get("updated_at"))
    ))
    conn.commit(); cur.close()
    upsert_raw(conn, "views", int(v["id"]), v.get("updated_at"), v)

def sync_triggers(conn):
    url = f"{Z_BASE}/api/v2/triggers.json"
    params = {"per_page": min(PER_PAGE, 100)}
    page = get_with_retry(url, params=params)
    while True:
        for t in page.get("triggers", []):
            upsert_trigger(conn, t)
        next_page = page.get("next_page") or page.get("links", {}).get("next")
        if not next_page:
            break
        page = get_with_retry(next_page)
    logging.info("Triggers: sync complete.")

def upsert_trigger(conn, t: Dict[str, Any]):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO triggers (id, title, description, active, position, category_id, raw_title, default_trigger,
                              conditions_json, actions_json, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
           title=VALUES(title), description=VALUES(description), active=VALUES(active), position=VALUES(position),
           category_id=VALUES(category_id), raw_title=VALUES(raw_title), default_trigger=VALUES(default_trigger),
           conditions_json=VALUES(conditions_json), actions_json=VALUES(actions_json),
           created_at=VALUES(created_at), updated_at=VALUES(updated_at)
    """, (
        t.get("id"), t.get("title"), t.get("description"), 1 if t.get("active") else 0, t.get("position"),
        t.get("category_id"), t.get("raw_title"), 1 if t.get("default") else 0,
        json.dumps(t.get("conditions") or {}), json.dumps(t.get("actions") or []),
        parse_dt(t.get("created_at")), parse_dt(t.get("updated_at"))
    ))
    conn.commit(); cur.close()
    upsert_raw(conn, "triggers", int(t["id"]), t.get("updated_at"), t)

def sync_trigger_categories(conn):
    url = f"{Z_BASE}/api/v2/trigger_categories.json"
    params = {"per_page": min(PER_PAGE, 100)}
    page = get_with_retry(url, params=params)
    while True:
        for c in page.get("trigger_categories", []):
            upsert_trigger_category(conn, c)
        next_page = page.get("next_page") or page.get("links", {}).get("next")
        if not next_page:
            break
        page = get_with_retry(next_page)
    logging.info("Trigger categories: sync complete.")

def upsert_trigger_category(conn, c: Dict[str, Any]):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO trigger_categories (id, name, position, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
           name=VALUES(name), position=VALUES(position), created_at=VALUES(created_at), updated_at=VALUES(updated_at)
    """, (str(c.get("id")), c.get("name"), c.get("position"),
          parse_dt(c.get("created_at")), parse_dt(c.get("updated_at"))))
    conn.commit(); cur.close()
    upsert_raw(conn, "trigger_categories", int(c["id"]), c.get("updated_at"), c)

def sync_macros(conn):
    url = f"{Z_BASE}/api/v2/macros.json"
    params = {"per_page": min(PER_PAGE, 100)}
    if INCLUDE:
        params["include"] = INCLUDE
    page = get_with_retry(url, params=params)
    while True:
        for m in page.get("macros", []):
            upsert_macro(conn, m)
        next_page = page.get("next_page") or page.get("links", {}).get("next")
        if not next_page:
            break
        page = get_with_retry(next_page)
    logging.info("Macros: sync complete.")

def upsert_macro(conn, m: Dict[str, Any]):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO macros (id, title, description, active, position, default_macro, restriction_json, actions_json, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
           title=VALUES(title), description=VALUES(description), active=VALUES(active), position=VALUES(position),
           default_macro=VALUES(default_macro), restriction_json=VALUES(restriction_json),
           actions_json=VALUES(actions_json), created_at=VALUES(created_at), updated_at=VALUES(updated_at)
    """, (
        m.get("id"), m.get("title"), m.get("description"), 1 if m.get("active") else 0, m.get("position"),
        1 if m.get("default") else 0, json.dumps(m.get("restriction")), json.dumps(m.get("actions") or []),
        parse_dt(m.get("created_at")), parse_dt(m.get("updated_at"))
    ))
    conn.commit(); cur.close()
    upsert_raw(conn, "macros", int(m["id"]), m.get("updated_at"), m)

# ----------------------------
# Main
# ----------------------------
def main():
    if DOWNLOAD_ATTACHMENTS:
        ensure_dir(ATTACHMENTS_DIR)
        logging.info("Attachment download enabled → %s", ATTACHMENTS_DIR)

    init_schema()
    conn = get_db()

    # order helps references
    sync_users(conn)
    sync_organizations(conn)

    # tickets + comments/attachments
    sync_tickets_comments_attachments(conn)
    if USE_TICKET_EVENTS_FOR_COMMENTS:
        sync_ticket_events_for_comments(conn)

    # metadata snapshots
    sync_views(conn)
    sync_triggers(conn)
    sync_trigger_categories(conn)
    sync_macros(conn)

    conn.close()
    logging.info("✅ Zendesk incremental backup complete.")

if __name__ == "__main__":
    main()
