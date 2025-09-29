from flask import Flask, render_template, request, send_from_directory, abort, url_for
import mysql.connector
import os, json
from dotenv import load_dotenv
from math import ceil
from pathlib import Path
from typing import Optional

load_dotenv()

DB_CFG = {
    "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "database": os.getenv("MYSQL_DB", "zendesk_backup"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASS"),
    "autocommit": True,
}

ATTACHMENTS_DIR = os.path.abspath(os.getenv("ATTACHMENTS_DIR", "./attachments"))

app = Flask(__name__)

def db():
    return mysql.connector.connect(**DB_CFG)

# ---------- Jinja helpers ----------
@app.template_filter("basename")
def basename_filter(p: Optional[str]) -> str:
    return os.path.basename(p) if p else ""

@app.template_filter("prettyjson")
def prettyjson_filter(obj) -> str:
    try:
        if isinstance(obj, str):
            obj = json.loads(obj)
        return json.dumps(obj, indent=2, ensure_ascii=False)
    except Exception:
        return str(obj)

@app.context_processor
def inject_globals():
    status_badges = {
        "new":    "bg-sky-100 text-sky-800 ring-1 ring-sky-200",
        "open":   "bg-amber-100 text-amber-800 ring-1 ring-amber-200",
        "pending":"bg-rose-100 text-rose-800 ring-1 ring-rose-200",
        "hold":   "bg-zinc-200 text-zinc-900 ring-1 ring-zinc-300",
        "solved": "bg-emerald-100 text-emerald-800 ring-1 ring-emerald-200",
        "closed": "bg-slate-200 text-slate-900 ring-1 ring-slate-300",
    }
    return {
        "ATTACHMENTS_DIR": ATTACHMENTS_DIR,
        "status_badges": status_badges,
    }

# ---------- Home ----------
@app.route("/")
def home():
    return render_template("home.html")

# ---------- Tickets ----------
@app.route("/tickets")
def tickets():
    page = max(int(request.args.get("page", 1)), 1)
    per_page = min(max(int(request.args.get("per_page", 25)), 1), 200)
    status = (request.args.get("status") or "").strip().lower()
    q = (request.args.get("q") or "").strip()

    where = []
    params = []
    if status:
        where.append("LOWER(status)=%s")
        params.append(status)
    if q:
        where.append("(subject LIKE %s OR description LIKE %s)")
        params.extend([f"%{q}%", f"%{q}%"])

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    offset = (page - 1) * per_page

    cn = db(); cur = cn.cursor(dictionary=True)
    cur.execute(f"SELECT COUNT(*) AS c FROM tickets {where_sql}", params)
    total = cur.fetchone()["c"]

    cur.execute(
        f"""SELECT id, subject, status, requester_id, assignee_id, organization_id,
                   created_at, updated_at
            FROM tickets
            {where_sql}
            ORDER BY updated_at DESC
            LIMIT %s OFFSET %s""",
        params + [per_page, offset],
    )
    rows = cur.fetchall()
    cur.close(); cn.close()

    return render_template(
        "tickets.html",
        tickets=rows,
        page=page,
        per_page=per_page,
        total=total,
        total_pages=max(1, ceil(total / per_page)),
        status=status,
        q=q
    )

@app.route("/tickets/<int:ticket_id>")
def ticket_detail(ticket_id: int):
    cn = db(); cur = cn.cursor(dictionary=True)

    cur.execute(
        """SELECT id, subject, description, status, priority, type,
                  requester_id, assignee_id, organization_id,
                  created_at, updated_at, due_at
           FROM tickets WHERE id=%s""",
        (ticket_id,),
    )
    ticket = cur.fetchone()
    if not ticket:
        cur.close(); cn.close()
        abort(404)

    cur.execute(
        """SELECT id, author_id, public, body, created_at, updated_at
           FROM ticket_comments
           WHERE ticket_id=%s
           ORDER BY created_at ASC""",
        (ticket_id,),
    )
    comments = cur.fetchall()

    cur.execute(
        """SELECT id, file_name, content_url, local_path, content_type, size, created_at
           FROM attachments
           WHERE ticket_id=%s
           ORDER BY created_at ASC, id ASC""",
        (ticket_id,),
    )
    attachments = cur.fetchall()
    cur.close(); cn.close()

    for a in attachments:
        lp = a.get("local_path")
        a["exists_local"] = bool(lp and Path(lp).exists())
        a["local_url"] = url_for("attachment_file", ticket_id=ticket_id, filename=os.path.basename(lp)) if a["exists_local"] else None

    return render_template("ticket_detail.html", ticket=ticket, comments=comments, attachments=attachments)

# ---------- Attachments ----------
@app.route("/attachments")
def attachments_list():
    page = max(int(request.args.get("page", 1)), 1)
    per_page = min(max(int(request.args.get("per_page", 25)), 1), 200)
    ticket_id = (request.args.get("ticket_id") or "").strip()
    q = (request.args.get("q") or "").strip()  # search filename

    where = []
    params = []
    if ticket_id.isdigit():
        where.append("ticket_id=%s")
        params.append(int(ticket_id))
    if q:
        where.append("file_name LIKE %s")
        params.append(f"%{q}%")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    offset = (page - 1) * per_page

    cn = db(); cur = cn.cursor(dictionary=True)
    cur.execute(f"SELECT COUNT(*) AS c FROM attachments {where_sql}", params)
    total = cur.fetchone()["c"]

    cur.execute(
        f"""SELECT id, ticket_id, comment_id, file_name, content_url, local_path,
                   content_type, size, created_at
            FROM attachments
            {where_sql}
            ORDER BY created_at DESC, id DESC
            LIMIT %s OFFSET %s""",
        params + [per_page, offset],
    )
    rows = cur.fetchall()
    cur.close(); cn.close()

    # existence check
    for a in rows:
        lp = a.get("local_path")
        a["exists_local"] = bool(lp and Path(lp).exists())
        if a["exists_local"] and a.get("ticket_id"):
            a["local_url"] = url_for("attachment_file", ticket_id=a["ticket_id"], filename=os.path.basename(lp))
        else:
            a["local_url"] = None

    return render_template(
        "attachments.html",
        attachments=rows,
        page=page,
        per_page=per_page,
        total=total,
        total_pages=max(1, ceil(total / per_page)),
        ticket_id=ticket_id,
        q=q
    )

@app.route("/attachments/<int:ticket_id>/<path:filename>")
def attachment_file(ticket_id: int, filename: str):
    base = os.path.join(ATTACHMENTS_DIR, str(ticket_id))
    path = os.path.join(base, filename)
    if os.path.commonpath([os.path.abspath(path), os.path.abspath(base)]) != os.path.abspath(base):
        abort(403)
    if not os.path.exists(path):
        abort(404)
    return send_from_directory(base, filename, as_attachment=True)

# ---------- Organizations ----------
@app.route("/organizations")
def organizations_list():
    page = max(int(request.args.get("page", 1)), 1)
    per_page = min(max(int(request.args.get("per_page", 25)), 1), 200)
    q = (request.args.get("q") or "").strip()

    where = []
    params = []
    if q:
        where.append("(name LIKE %s OR external_id LIKE %s)")
        params += [f"%{q}%", f"%{q}%"]
    where_sql = "WHERE " + " AND ".join(where) if where else ""
    offset = (page - 1) * per_page

    cn = db(); cur = cn.cursor(dictionary=True)
    cur.execute(f"SELECT COUNT(*) AS c FROM organizations {where_sql}", params)
    total = cur.fetchone()["c"]

    cur.execute(
        f"""SELECT id, name, external_id, group_id, created_at, updated_at
            FROM organizations
            {where_sql}
            ORDER BY updated_at DESC, id DESC
            LIMIT %s OFFSET %s""",
        params + [per_page, offset],
    )
    rows = cur.fetchall()

    # raw json (optional on detail only)
    cur.close(); cn.close()

    return render_template("organizations.html",
                           orgs=rows, page=page, per_page=per_page, total=total,
                           total_pages=max(1, ceil(total / per_page)), q=q)

@app.route("/organizations/<int:org_id>")
def organization_detail(org_id: int):
    cn = db(); cur = cn.cursor(dictionary=True)
    cur.execute("""SELECT * FROM organizations WHERE id=%s""", (org_id,))
    org = cur.fetchone()
    if not org:
        cur.close(); cn.close(); abort(404)
    # show raw snapshot if present
    cur.execute("""SELECT payload_json FROM raw_snapshots WHERE resource='organizations' AND entity_id=%s""", (org_id,))
    raw = cur.fetchone()
    raw_json = raw["payload_json"] if raw else None
    cur.close(); cn.close()
    return render_template("organization_detail.html", org=org, raw_json=raw_json)

# ---------- Users ----------
@app.route("/users")
def users_list():
    page = max(int(request.args.get("page", 1)), 1)
    per_page = min(max(int(request.args.get("per_page", 25)), 1), 200)
    q = (request.args.get("q") or "").strip()

    where = []
    params = []
    if q:
        where.append("(name LIKE %s OR email LIKE %s)")
        params += [f"%{q}%", f"%{q}%"]
    where_sql = "WHERE " + " AND ".join(where) if where else ""
    offset = (page - 1) * per_page

    cn = db(); cur = cn.cursor(dictionary=True)
    cur.execute(f"SELECT COUNT(*) AS c FROM users {where_sql}", params)
    total = cur.fetchone()["c"]

    cur.execute(
        f"""SELECT id, name, email, role, active, suspended, organization_id, updated_at
            FROM users
            {where_sql}
            ORDER BY updated_at DESC, id DESC
            LIMIT %s OFFSET %s""",
        params + [per_page, offset],
    )
    rows = cur.fetchall()
    cur.close(); cn.close()

    return render_template("users.html", users=rows, page=page, per_page=per_page,
                           total=total, total_pages=max(1, ceil(total / per_page)), q=q)

@app.route("/users/<int:user_id>")
def user_detail(user_id: int):
    cn = db(); cur = cn.cursor(dictionary=True)
    cur.execute("""SELECT * FROM users WHERE id=%s""", (user_id,))
    user = cur.fetchone()
    if not user:
        cur.close(); cn.close(); abort(404)
    cur.execute("""SELECT payload_json FROM raw_snapshots WHERE resource='users' AND entity_id=%s""", (user_id,))
    raw = cur.fetchone()
    raw_json = raw["payload_json"] if raw else None
    cur.close(); cn.close()
    return render_template("user_detail.html", user=user, raw_json=raw_json)

# ---------- Triggers ----------
@app.route("/triggers")
def triggers_list():
    page = max(int(request.args.get("page", 1)), 1)
    per_page = min(max(int(request.args.get("per_page", 25)), 1), 200)
    q = (request.args.get("q") or "").strip()

    where = []
    params = []
    if q:
        where.append("(title LIKE %s OR description LIKE %s)")
        params += [f"%{q}%", f"%{q}%"]
    where_sql = "WHERE " + " AND ".join(where) if where else ""
    offset = (page - 1) * per_page

    cn = db(); cur = cn.cursor(dictionary=True)
    cur.execute(f"SELECT COUNT(*) AS c FROM triggers {where_sql}", params)
    total = cur.fetchone()["c"]

    cur.execute(
        f"""SELECT id, title, active, position, category_id, updated_at
            FROM triggers
            {where_sql}
            ORDER BY updated_at DESC, id DESC
            LIMIT %s OFFSET %s""",
        params + [per_page, offset],
    )
    rows = cur.fetchall()
    cur.close(); cn.close()

    return render_template("triggers.html", triggers=rows, page=page, per_page=per_page,
                           total=total, total_pages=max(1, ceil(total / per_page)), q=q)

@app.route("/triggers/<int:trigger_id>")
def trigger_detail(trigger_id: int):
    cn = db(); cur = cn.cursor(dictionary=True)
    cur.execute("""SELECT * FROM triggers WHERE id=%s""", (trigger_id,))
    trig = cur.fetchone()
    if not trig:
        cur.close(); cn.close(); abort(404)
    cur.execute("""SELECT payload_json FROM raw_snapshots WHERE resource='triggers' AND entity_id=%s""", (trigger_id,))
    raw = cur.fetchone()
    raw_json = raw["payload_json"] if raw else None
    cur.close(); cn.close()
    return render_template("trigger_detail.html", trig=trig, raw_json=raw_json)

# ---------- Macros ----------
@app.route("/macros")
def macros_list():
    page = max(int(request.args.get("page", 1)), 1)
    per_page = min(max(int(request.args.get("per_page", 25)), 1), 200)
    q = (request.args.get("q") or "").strip()

    where = []
    params = []
    if q:
        where.append("(title LIKE %s OR description LIKE %s)")
        params += [f"%{q}%", f"%{q}%"]
    where_sql = "WHERE " + " AND ".join(where) if where else ""
    offset = (page - 1) * per_page

    cn = db(); cur = cn.cursor(dictionary=True)
    cur.execute(f"SELECT COUNT(*) AS c FROM macros {where_sql}", params)
    total = cur.fetchone()["c"]

    cur.execute(
        f"""SELECT id, title, active, position, updated_at
            FROM macros
            {where_sql}
            ORDER BY updated_at DESC, id DESC
            LIMIT %s OFFSET %s""",
        params + [per_page, offset],
    )
    rows = cur.fetchall()
    cur.close(); cn.close()

    return render_template("macros.html", macros=rows, page=page, per_page=per_page,
                           total=total, total_pages=max(1, ceil(total / per_page)), q=q)

@app.route("/macros/<int:macro_id>")
def macro_detail(macro_id: int):
    cn = db(); cur = cn.cursor(dictionary=True)
    cur.execute("""SELECT * FROM macros WHERE id=%s""", (macro_id,))
    macro = cur.fetchone()
    if not macro:
        cur.close(); cn.close(); abort(404)
    cur.execute("""SELECT payload_json FROM raw_snapshots WHERE resource='macros' AND entity_id=%s""", (macro_id,))
    raw = cur.fetchone()
    raw_json = raw["payload_json"] if raw else None
    cur.close(); cn.close()
    return render_template("macro_detail.html", macro=macro, raw_json=raw_json)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5050, debug=True)
