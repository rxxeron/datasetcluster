"""Cloud Live Tracker for multi-machine PDF parsing.

Enables multiple laptops to synchronize in real time via a Google Sheet Webhook API.
Ensures zero duplicate work and atomic claiming across machines.
"""

import json
import logging
import os
import socket
import time
import urllib.error
import urllib.parse
import urllib.request

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "tracker_config.json")
logger = logging.getLogger("cloud_tracker")


class CloudTracker:
    def __init__(self, config_path: str = CONFIG_FILE, worker_override: str = ""):
        self.config_path = config_path
        self.config = self._load_config()
        self.worker_name = worker_override or self.config.get("worker_name") or socket.gethostname()
        self.webhook_url = (self.config.get("google_sheet_webhook_url") or "").strip()
        self.enabled = bool(self.config.get("enabled", True) and self.webhook_url)
        self.cache_ttl = int(self.config.get("cache_ttl_sec", 60))
        self._cache = {}
        self._cache_timestamp = 0.0

        if not self.enabled:
            logger.info("CloudTracker: Google Sheet Webhook URL not configured. Operating in local mode.")
        else:
            logger.info(f"CloudTracker: Active on machine '{self.worker_name}' -> {self.webhook_url}")

    def _load_config(self) -> dict:
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not read tracker config: {e}")
        return {
            "google_sheet_webhook_url": "",
            "worker_name": socket.gethostname(),
            "enabled": True,
            "cache_ttl_sec": 60,
        }

    def _call_api(self, params: dict, payload: dict | None = None, timeout: int = 15) -> dict:
        if not self.enabled:
            return {"success": False, "reason": "disabled"}

        query = urllib.parse.urlencode(params)
        url = f"{self.webhook_url}?{query}" if query else self.webhook_url

        try:
            if payload is not None:
                body = json.dumps(payload).encode("utf-8")
                req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
            else:
                req = urllib.request.Request(url)

            # urllib automatically follows HTTP 302 redirects issued by Google Apps Script
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                resp_text = resp.read().decode("utf-8")
                return json.loads(resp_text)
        except urllib.error.HTTPError as e:
            logger.warning(f"CloudTracker HTTP error: {e.code} {e.reason}")
            return {"success": False, "error": str(e)}
        except Exception as e:
            logger.warning(f"CloudTracker network error: {e}")
            return {"success": False, "error": str(e)}

    def refresh_cache(self, force: bool = False) -> dict:
        """Fetches the latest snapshot of all files and their statuses from the Google Sheet."""
        if not self.enabled:
            return {}

        now = time.time()
        if not force and (now - self._cache_timestamp) < self.cache_ttl:
            return self._cache

        resp = self._call_api({"action": "dump"}, timeout=20)
        self._cache_timestamp = now
        if resp.get("success") and isinstance(resp.get("data"), dict):
            self._cache = resp["data"]
            logger.info(f"CloudTracker: Synced {len(self._cache)} statuses from Google Sheet.")
        return self._cache

    def is_already_completed(self, drive_file_id: str) -> bool:
        """Checks if a file is marked COMPLETED on the cloud tracker."""
        if not self.enabled:
            return False

        cache = self.refresh_cache()
        item = cache.get(drive_file_id)
        if item and item.get("status") == "COMPLETED":
            return True
        return False

    def is_claimed_by_other(self, drive_file_id: str) -> tuple[bool, str]:
        """Checks if another worker machine has claimed this file."""
        if not self.enabled:
            return False, ""

        cache = self.refresh_cache()
        item = cache.get(drive_file_id)
        if not item:
            return False, ""

        status = item.get("status")
        worker = item.get("worker", "")
        if status == "IN_PROGRESS" and worker and worker != self.worker_name:
            return True, worker
        return False, ""

    def claim(self, drive_file_id: str) -> bool:
        """Atomically claims the file on the Google Sheet.
        Returns True if claim succeeded, False if already completed or locked by another machine.
        """
        if not self.enabled:
            return True  # Local mode allows execution

        resp = self._call_api({
            "action": "claim",
            "id": drive_file_id,
            "worker": self.worker_name,
        }, timeout=15)

        if resp.get("success"):
            # Update local memory cache
            self._cache[drive_file_id] = {
                "status": "IN_PROGRESS",
                "worker": self.worker_name,
                "claimed_at": resp.get("claimed_at", ""),
            }
            logger.info(f"[CLAIMED] Locked '{drive_file_id}' for worker '{self.worker_name}' on Google Sheet.")
            return True
        else:
            reason = resp.get("reason", "unknown")
            other_worker = resp.get("worker", "another laptop")
            logger.warning(f"[CLAIM DENIED] Could not claim '{drive_file_id}': {reason} (Holder: {other_worker})")
            return False

    def mark_completed(self, drive_file_id: str, total_pages: int = 0, completed_pages: int = 0, output_json: str = "") -> bool:
        """Reports completion to Google Sheet."""
        if not self.enabled:
            return True

        resp = self._call_api({
            "action": "complete",
            "id": drive_file_id,
            "worker": self.worker_name,
            "total_pages": total_pages,
            "completed_pages": completed_pages,
            "output_json": output_json,
        }, timeout=15)

        if resp.get("success"):
            self._cache[drive_file_id] = {
                "status": "COMPLETED",
                "worker": self.worker_name,
                "total_pages": total_pages,
                "completed_pages": completed_pages,
            }
            logger.info(f"[COMPLETED] Reported '{drive_file_id}' completed on Google Sheet.")
            return True
        return False

    def mark_failed(self, drive_file_id: str, error_msg: str = "") -> bool:
        """Reports failure to Google Sheet."""
        if not self.enabled:
            return True

        resp = self._call_api({
            "action": "fail",
            "id": drive_file_id,
            "worker": self.worker_name,
            "error": error_msg,
        }, timeout=15)

        if resp.get("success"):
            self._cache[drive_file_id] = {
                "status": "FAILED",
                "worker": self.worker_name,
                "error": error_msg,
            }
            return True
        return False


# Singleton instance helper
_instance = None


def get_tracker(worker_name: str = "") -> CloudTracker:
    global _instance
    if _instance is None or (worker_name and _instance.worker_name != worker_name):
        _instance = CloudTracker(worker_override=worker_name)
    return _instance
