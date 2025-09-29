# api/backup.py
import os
import json

# Force-disable attachment downloads on Vercel (ephemeral FS, no disk persistence)
os.environ.setdefault("DOWNLOAD_ATTACHMENTS", "false")

from backup import main as run_backup

def handler(request):
    try:
        run_backup()
        return (
            json.dumps({"ok": True, "message": "Zendesk backup finished"}),
            200,
            {"Content-Type": "application/json"}
        )
    except Exception as e:
        return (
            json.dumps({"ok": False, "error": str(e)}),
            500,
            {"Content-Type": "application/json"}
        )
