"""
PREP404 Google Drive Uploader & Folder Synchronizer.
Mirrors local disk hierarchy into Google Drive via Google Apps Script Web App:
Root: aapathshala_parsed / <collection> / <book_slug> / book.json
"""

import argparse
import glob
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional

logger = logging.getLogger("gdrive_uploader")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", "%H:%M:%S"))
logger.addHandler(ch)

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "tracker_config.json")
MANIFEST_PATH = os.path.join(os.path.dirname(__file__), "manifest.json")


def load_webhook_url() -> str:
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                return (data.get("google_sheet_webhook_url") or "").strip()
        except Exception:
            pass
    return "https://script.google.com/macros/s/AKfycbyCfy3uARDlX1PD13369W0uFzJPOcHbgq31uRGIrKLj8Evbv0csdsftfevhWjCUDVc/exec"


def call_apps_script(webhook_url: str, payload: Dict[str, Any], timeout_sec: int = 60) -> Dict[str, Any]:
    """Posts payload to Google Apps Script Web App following 302 redirects."""
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=body,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            data_bytes = resp.read()
            return json.loads(data_bytes.decode("utf-8", errors="ignore"))
    except Exception as e:
        logger.error(f"Apps Script call error: {e}")
        return {"success": False, "error": str(e)}


def init_all_drive_folders(webhook_url: str, chunk_size: int = 250):
    """Sends manifest items in chunks to create the entire folder hierarchy in Google Drive."""
    if not os.path.exists(MANIFEST_PATH):
        logger.error(f"manifest.json not found at {MANIFEST_PATH}")
        return

    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
        items = data.get("items", [])

    logger.info(f"Loaded {len(items)} items from manifest. Initializing Google Drive folders...")
    
    clean_items = []
    for it in items:
        clean_items.append({
            "id": it.get("drive_file_id"),
            "collection": it.get("collection", "unspecified"),
            "book_slug": it.get("book_slug", it.get("drive_file_id")),
        })

    for i in range(0, len(clean_items), chunk_size):
        chunk = clean_items[i:i + chunk_size]
        logger.info(f"Sending batch {i + 1} - {min(i + len(chunk), len(clean_items))} of {len(clean_items)}...")
        res = call_apps_script(webhook_url, {"action": "create_folders", "items": chunk}, timeout_sec=90)
        if res.get("success"):
            logger.info(f"  -> Batch created successfully. Root Folder: {res.get('rootFolderUrl')}")
        else:
            logger.warning(f"  -> Batch response: {res}")
        time.sleep(1.0)

    logger.info("🎉 Google Drive folder structure initialization complete!")


def upload_single_book_json(webhook_url: str, book_json_path: str, collection: str, book_slug: str, drive_id: Optional[str] = None, worker: str = "SyncScript") -> bool:
    """Uploads a verified book.json to its Google Drive folder."""
    if not os.path.exists(book_json_path):
        logger.error(f"File not found: {book_json_path}")
        return False

    with open(book_json_path, "r", encoding="utf-8") as f:
        content_str = f.read()

    try:
        parsed_data = json.loads(content_str)
        total_pages = parsed_data.get("total_pages", len(parsed_data.get("pages", [])))
        completed_pages = len(parsed_data.get("pages", []))
    except Exception:
        total_pages = 0
        completed_pages = 0

    payload = {
        "action": "upload_book",
        "id": drive_id or book_slug,
        "collection": collection,
        "book_slug": book_slug,
        "content": content_str,
        "total_pages": total_pages,
        "completed_pages": completed_pages,
        "worker": worker,
    }

    res = call_apps_script(webhook_url, payload, timeout_sec=90)
    if res.get("success"):
        logger.info(f"✅ [GDRIVE UPLOAD] {collection}/{book_slug} -> {res.get('drive_file_url')}")
        return True
    else:
        logger.warning(f"❌ [GDRIVE FAIL] {collection}/{book_slug} -> {res.get('error') or res.get('reason')}")
        return False


def sync_local_parsed_to_gdrive(webhook_url: str, root_parsed_dir: str = r"D:\Books\aapathshala_parsed"):
    """Scans local disk for verified book.json files and uploads them into Google Drive."""
    pattern = os.path.join(root_parsed_dir, "*", "*", "book.json")
    files = glob.glob(pattern)
    logger.info(f"Found {len(files)} completed book.json files on local disk ({root_parsed_dir}). Syncing...")

    success_count = 0
    for idx, fpath in enumerate(files, 1):
        rel_parts = os.path.relpath(fpath, root_parsed_dir).split(os.sep)
        if len(rel_parts) >= 3:
            col = rel_parts[0]
            slug = rel_parts[1]
            # Extract drive id from slug if possible (last 8+ chars)
            drive_id = slug.split("_")[-1] if "_" in slug else slug
            logger.info(f"[{idx}/{len(files)}] Uploading {col}/{slug}...")
            ok = upload_single_book_json(webhook_url, fpath, collection=col, book_slug=slug, drive_id=drive_id)
            if ok:
                success_count += 1
            time.sleep(0.5)

    logger.info(f"🎉 Sync Complete! Successfully uploaded {success_count}/{len(files)} books to Google Drive.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PREP404 Google Drive Uploader")
    parser.add_argument("--init-folders", action="store_true", help="Create folder tree in Google Drive for all 1,496 books")
    parser.add_argument("--sync-local", action="store_true", help="Sync all local completed book.json files to Google Drive")
    parser.add_argument("--file", type=str, help="Upload a specific book.json")
    parser.add_argument("--col", type=str, help="Collection name for single file")
    parser.add_argument("--slug", type=str, help="Book slug for single file")
    args = parser.parse_args()

    url = load_webhook_url()
    if not url:
        logger.error("No webhook URL configured.")
        sys.exit(1)

    if args.init_folders:
        init_all_drive_folders(url)
    elif args.sync_local:
        sync_local_parsed_to_gdrive(url)
    elif args.file and args.col and args.slug:
        upload_single_book_json(url, args.file, args.col, args.slug)
    else:
        parser.print_help()
