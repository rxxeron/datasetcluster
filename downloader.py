"""Ephemeral Google Drive PDF downloader with large-file confirm handling and cleanup."""

import hashlib
import html
import http.cookiejar
import os
import re
import socket
import time
import urllib.parse
import urllib.request

socket.setdefaulttimeout(90.0)

from config import TMP_DIR, ensure_dirs


def _build_opener() -> urllib.request.OpenerDirector:
    cj = http.cookiejar.CookieJar()
    return urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))


def _extract_confirm_url(html_text: str, file_id: str) -> str:
    """Parse Google Drive virus-scan warning HTML form into a direct download URL."""
    action_m = re.search(r'<form[^>]+id="download-form"[^>]+action="([^"]+)"', html_text)
    action = html.unescape(action_m.group(1)) if action_m else "https://drive.usercontent.google.com/download"
    params: dict[str, str] = {}
    for name, val in re.findall(r'<input[^>]+type="hidden"[^>]+name="([^"]+)"[^>]+value="([^"]*)"', html_text):
        params[name] = html.unescape(val)
    if "id" not in params:
        params["id"] = file_id
    if "export" not in params:
        params["export"] = "download"
    if "confirm" not in params:
        params["confirm"] = "t"
    return f"{action}?{urllib.parse.urlencode(params)}"


def download_drive_pdf(file_id: str, retries: int = 3, timeout_sec: int = 90) -> tuple[str, str, int]:
    """Download a Drive PDF to TMP_DIR/<file_id>.pdf; returns (local_path, sha256, size_bytes)."""
    ensure_dirs()
    final_path = os.path.join(TMP_DIR, f"{file_id}.pdf")
    part_path = final_path + ".part"

    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            opener = _build_opener()
            url = f"https://drive.google.com/uc?export=download&id={file_id}"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with opener.open(req, timeout=timeout_sec) as resp:
                first_chunk = resp.read(65536)
                if not first_chunk.startswith(b"%PDF"):
                    rest = resp.read(524288)
                    page_html = (first_chunk + rest).decode("utf-8", errors="ignore")
                    if "download-form" in page_html or "uc-download-link" in page_html:
                        confirm_url = _extract_confirm_url(page_html, file_id)
                        req2 = urllib.request.Request(confirm_url, headers={"User-Agent": "Mozilla/5.0"})
                        return _stream_to_file(opener, req2, part_path, final_path, timeout_sec)
                    raise ValueError(f"Drive returned non-PDF response for {file_id}: {page_html[:180]!r}")

                return _write_stream(resp, first_chunk, part_path, final_path)
        except Exception as exc:
            last_err = exc
            if os.path.exists(part_path):
                try:
                    os.remove(part_path)
                except OSError:
                    pass
            time.sleep(2.0 * (attempt + 1))

    raise RuntimeError(f"Failed downloading Drive file {file_id} after {retries} attempts: {last_err}")


def _stream_to_file(opener, req, part_path: str, final_path: str, timeout_sec: int) -> tuple[str, str, int]:
    with opener.open(req, timeout=timeout_sec) as resp:
        first_chunk = resp.read(65536)
        if not first_chunk.startswith(b"%PDF"):
            raise ValueError(f"Confirmed Drive response is not a PDF: {first_chunk[:120]!r}")
        return _write_stream(resp, first_chunk, part_path, final_path)


def _write_stream(resp, first_chunk: bytes, part_path: str, final_path: str) -> tuple[str, str, int]:
    h = hashlib.sha256()
    size = 0
    with open(part_path, "wb") as f:
        f.write(first_chunk)
        h.update(first_chunk)
        size += len(first_chunk)
        while True:
            buf = resp.read(262144)
            if not buf:
                break
            f.write(buf)
            h.update(buf)
            size += len(buf)
    os.replace(part_path, final_path)
    return final_path, h.hexdigest(), size


def compute_sha256(path: str) -> tuple[str, int]:
    """Compute SHA-256 and byte size for an existing local PDF."""
    h = hashlib.sha256()
    size = 0
    with open(path, "rb") as f:
        while True:
            buf = f.read(262144)
            if not buf:
                break
            h.update(buf)
            size += len(buf)
    return h.hexdigest(), size


def cleanup_temp_file(path: str | None) -> None:
    """Safely delete a temporary file only if it lives inside TMP_DIR."""
    if not path:
        return
    abs_tmp = os.path.abspath(TMP_DIR)
    abs_target = os.path.abspath(path)
    if abs_target.startswith(abs_tmp + os.sep) and os.path.exists(abs_target):
        try:
            os.remove(abs_target)
        except OSError:
            pass
