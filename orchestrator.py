"""
AAPathshala Orchestrator: High-Throughput Cluster Parser Engine.
Consumes healthy slots from Sentinel, dispatches vision OCR tasks,
handles adaptive dense page slicing, and strictly validates 100% clean output.
"""

import base64
import concurrent.futures
import datetime
import json
import logging
import os
import re
import threading
import time
import urllib.error
import urllib.request
import pymupdf

from cloud_tracker import get_tracker
from downloader import cleanup_temp_file, download_drive_pdf
from sentinel import sentinel

logger = logging.getLogger("orchestrator")
logger.setLevel(logging.INFO)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TMP_DIR = os.path.join(BASE_DIR, "_tmp")
PARSED_DIR = os.path.join(BASE_DIR, "parsed_output")
MANIFEST_FILE = os.path.join(BASE_DIR, "manifest.json")

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(PARSED_DIR, exist_ok=True)

OCR_PROMPT = """You are an expert OCR and transcription engine for Bangladeshi academic textbooks, question banks, and solution manuals.
Transcribe EVERYTHING printed on this page VERBATIM in its original language (keep all Bengali text in Bengali Unicode, never translate to English) + LaTeX ($...$ or $$...$$) for formulas and equations.
Include all:
- Chapter titles, headings, and sub-headings (#, ##, ###)
- Paragraph text, explanations, and notes
- Mathematical and chemical formulas
- Tables formatted as Markdown (| col1 | col2 |)
- Questions, MCQs with options (a, b, c, d), answers, and solutions
- If the page is blank, a pure decorative cover, or contains no readable text, output: NONE
Stop immediately after the last visible printed line."""


class ParsingOrchestrator:
    def __init__(self):
        self.paused = False
        self.running = False
        self._thread = None
        self.current_book = None
        self.book_progress = {"current_page": 0, "total_pages": 0, "completed_pages": 0}
        self.stats = {
            "total_books_catalog": 0,
            "completed_books": 0,
            "total_pages_parsed": 0,
            "start_time": time.time(),
            "recent_logs": [],
        }
        self.log_callbacks = []
        self._load_catalog_stats()

    def _load_catalog_stats(self):
        if os.path.exists(MANIFEST_FILE):
            try:
                with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    items = data.get("items", [])
                    self.stats["total_books_catalog"] = len(items)
                    self.stats["completed_books"] = sum(1 for it in items if it.get("status") == "completed")
            except Exception:
                pass

    def add_log_listener(self, cb):
        self.log_callbacks.append(cb)

    def log(self, msg: str, level: str = "INFO"):
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        entry = {"time": ts, "message": msg, "level": level}
        self.stats["recent_logs"].append(entry)
        if len(self.stats["recent_logs"]) > 200:
            self.stats["recent_logs"].pop(0)
        
        for cb in self.log_callbacks:
            try:
                cb(entry)
            except Exception:
                pass

    def call_remote_vlm(self, endpoint: dict, b64_img: str, timeout_sec: int = 180) -> tuple[str, float]:
        base_url = endpoint["base_url"].rstrip("/")
        model = endpoint.get("model", "qwen2.5vl:7b")
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": OCR_PROMPT, "images": [b64_img]}],
            "stream": False,
        }
        data_bytes = json.dumps(payload).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 Sentinel/2.0",
        }
        t0 = time.time()
        try:
            req = urllib.request.Request(f"{base_url}/api/chat", data=data_bytes, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
                res = json.loads(resp.read().decode("utf-8", errors="ignore"))
                content = res.get("message", {}).get("content", "").strip()
                elapsed = round(time.time() - t0, 1)
                return content, elapsed
        except Exception as e:
            elapsed = round(time.time() - t0, 1)
            return f"ERROR: {e}", elapsed

    def parse_page_task(self, idx: int, pno: int, total_pages: int, item_type: str, payload_data: any, page_json_path: str) -> dict:
        if os.path.exists(page_json_path):
            try:
                with open(page_json_path, "r", encoding="utf-8") as f:
                    cached = json.load(f)
                    if cached.get("raw_text") and not cached["raw_text"].startswith("ERROR:"):
                        self.log(f"⚡ [CACHED] p{pno:03d}/{total_pages:03d} -> loaded from disk ({len(cached['raw_text'])} chars)")
                        self.book_progress["completed_pages"] += 1
                        return cached
            except Exception:
                pass

        if item_type == "digital_text":
            page_data = {
                "page_number": pno,
                "raw_text": payload_data,
                "is_blank_or_cover": False,
                "char_count": len(payload_data),
                "extraction_mode": "digital_text",
                "model": "fast_extract",
                "node": "local",
                "elapsed_sec": 0.0,
                "extracted_at": datetime.datetime.now().isoformat(),
            }
            with open(page_json_path, "w", encoding="utf-8") as f:
                json.dump(page_data, f, ensure_ascii=False, indent=2)
            self.log(f"📄 [DIGITAL] p{pno:03d}/{total_pages:03d} -> {len(payload_data)} chars (0.0s)")
            self.book_progress["completed_pages"] += 1
            return page_data

        b64_full = payload_data.get("full", payload_data) if isinstance(payload_data, dict) else payload_data

        ep = sentinel.acquire_slot(timeout=25.0)
        if not ep:
            return {
                "page_number": pno,
                "raw_text": "ERROR: All cluster slots offline/empty during processing.",
                "is_blank_or_cover": False,
                "char_count": 0,
                "extraction_mode": "vision_error",
                "model": "none",
                "node": "none",
                "elapsed_sec": 0.0,
                "extracted_at": datetime.datetime.now().isoformat(),
            }

        node_name = ep.get("name", "remote")
        timeout_sec = ep.get("timeout_sec", 180)
        self.log(f"-> [DISPATCH] p{pno:03d}/{total_pages:03d} sent to {node_name} [vision]...")

        try:
            content, elapsed = self.call_remote_vlm(ep, b64_full, timeout_sec=timeout_sec)
        finally:
            cooldown = 3.0 if ("HTTP Error 530" in content or "Connection refused" in content) else 0.0
            sentinel.release_slot(ep, cooldown=cooldown)

        # Retry loop across available slots
        retry_count = 0
        while content.startswith("ERROR:") and retry_count < 3:
            retry_count += 1
            self.log(f"⚠️ [WARNING] p{pno:03d}/{total_pages:03d} failed on {node_name} ({content[:35]}), retrying ({retry_count}/3)...", "WARN")
            ep_retry = sentinel.acquire_slot(timeout=10.0)
            if not ep_retry:
                break
            node_name = ep_retry.get("name", "remote")
            timeout_sec = ep_retry.get("timeout_sec", 180)
            self.log(f"-> [RETRY {retry_count}] p{pno:03d}/{total_pages:03d} sent to {node_name} [vision]...")
            try:
                content, elapsed = self.call_remote_vlm(ep_retry, b64_full, timeout_sec=timeout_sec)
            finally:
                cooldown = 3.0 if ("HTTP Error 530" in content or "Connection refused" in content) else 0.0
                sentinel.release_slot(ep_retry, cooldown=cooldown)

        is_fail = content.startswith("ERROR:")
        is_blank = (not is_fail) and (content.upper() in ("NONE", "NONE.", "BLANK", "EMPTY") or len(content.strip()) == 0)
        extraction_mode = "vision"

        # Adaptive Slicing for dense pages
        if not is_fail and not is_blank and isinstance(payload_data, dict) and "top" in payload_data and "bot" in payload_data:
            has_repetition = bool(re.search(r'([^\n]{10,80})\1{3,}', content))
            is_ceiling = len(content) >= 2350
            ends_abruptly = len(content) > 2000 and not content.rstrip().endswith((".", "।", "}", ")", ">", "|", "```", "---", "\n", "]"))

            if is_ceiling or has_repetition or ends_abruptly:
                reason = "repetition" if has_repetition else ("token ceiling (>2350 chars)" if is_ceiling else "abrupt cutoff")
                self.log(f"✂️ [DENSE SLICE] p{pno:03d}/{total_pages:03d} triggered ({reason}, was {len(content)} chars) -> slicing Top/Bot...")

                # Top slice
                ep_top = sentinel.acquire_slot(timeout=20.0)
                t_s0 = time.time()
                if ep_top:
                    top_node = ep_top.get("name", "remote")
                    self.log(f"  -> [SLICE TOP] p{pno:03d} sent to {top_node}...")
                    try:
                        top_txt, _ = self.call_remote_vlm(ep_top, payload_data["top"], timeout_sec=ep_top.get("timeout_sec", 180))
                    finally:
                        sentinel.release_slot(ep_top)
                else:
                    top_node = "none"
                    top_txt = "ERROR: No slot available for top slice"

                # Bot slice
                ep_bot = sentinel.acquire_slot(timeout=20.0)
                if ep_bot:
                    bot_node = ep_bot.get("name", "remote")
                    self.log(f"  -> [SLICE BOT] p{pno:03d} sent to {bot_node}...")
                    try:
                        bot_txt, _ = self.call_remote_vlm(ep_bot, payload_data["bot"], timeout_sec=ep_bot.get("timeout_sec", 180))
                    finally:
                        sentinel.release_slot(ep_bot)
                else:
                    bot_node = "none"
                    bot_txt = "ERROR: No slot available for bot slice"

                slice_el = round(time.time() - t_s0, 1)

                if not top_txt.startswith("ERROR:") and not bot_txt.startswith("ERROR:"):
                    top_clean = "" if top_txt.strip().upper() in ("NONE", "BLANK") else top_txt.strip()
                    bot_clean = "" if bot_txt.strip().upper() in ("NONE", "BLANK") else bot_txt.strip()
                    merged = f"{top_clean}\n\n---\n\n{bot_clean}".strip()
                    self.log(f"✅ [SLICED SUCCESS] p{pno:03d}/{total_pages:03d} in {slice_el}s: Top ({len(top_clean)}c) + Bot ({len(bot_clean)}c) => {len(merged)} chars!", "SUCCESS")
                    content = merged
                    elapsed = round(elapsed + slice_el, 1)
                    node_name = f"{top_node}+{bot_node}"
                    extraction_mode = "vision_sliced"
                else:
                    self.log(f"⚠️ [SLICE FALLBACK] Slicing error, keeping full-page output ({len(content)} chars)", "WARN")

        page_data = {
            "page_number": pno,
            "raw_text": content,
            "is_blank_or_cover": is_blank,
            "char_count": len(content),
            "extraction_mode": extraction_mode,
            "model": ep.get("model", "qwen2.5vl:7b"),
            "node": node_name,
            "elapsed_sec": elapsed,
            "extracted_at": datetime.datetime.now().isoformat(),
        }

        with open(page_json_path, "w", encoding="utf-8") as f:
            json.dump(page_data, f, ensure_ascii=False, indent=2)

        if is_fail:
            self.log(f"❌ [FAIL] p{pno:03d}/{total_pages:03d} on {node_name} in {elapsed}s: {content[:45]}", "ERROR")
        else:
            self.log(f"✅ [SUCCESS] p{pno:03d}/{total_pages:03d} on {node_name} in {elapsed}s -> {len(content)} chars", "SUCCESS")

        self.book_progress["completed_pages"] += 1
        return page_data

    def process_book(self, item: dict, tracker) -> bool:
        fid = item.get("drive_file_id")
        filename = item.get("filename", "unknown.pdf")
        col = item.get("collection", "unspecified")
        book_slug = item.get("book_slug", fid)

        book_dir = os.path.join(PARSED_DIR, col, book_slug)
        pages_dir = os.path.join(book_dir, "pages")
        book_json_path = os.path.join(book_dir, "book.json")

        os.makedirs(pages_dir, exist_ok=True)

        if os.path.exists(book_json_path):
            try:
                with open(book_json_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    has_errors = any((not p or p.get("raw_text", "").startswith("ERROR:")) for p in data.get("pages", []))
                    if not has_errors and len(data.get("pages", [])) > 0:
                        self.log(f"[DISK SKIP] {filename} already completed.")
                        return True
            except Exception:
                pass

        if tracker and tracker.enabled and tracker.is_already_completed(fid):
            self.log(f"[CLOUD SKIP] {filename} already marked completed on Google Sheet.")
            return True

        if tracker and tracker.enabled:
            claimed, claimant = tracker.claim(fid, book_title=filename, collection=col)
            if not claimed and claimant != tracker.worker_name:
                self.log(f"[CLAIM DENIED] Could not claim '{filename}' (Holder: {claimant}). Skipping.")
                return False

        self.log(f"📥 [DOWNLOAD] {filename} (id={fid})...")
        temp_pdf = download_drive_pdf(fid, filename)
        if not temp_pdf or not os.path.exists(temp_pdf):
            self.log(f"❌ [DOWNLOAD FAIL] {filename} could not be downloaded.", "ERROR")
            if tracker and tracker.enabled:
                tracker.mark_failed(fid, error_msg="Download failed")
            return False

        try:
            doc = pymupdf.open(temp_pdf)
            total_pages = len(doc)
            self.current_book = filename
            self.book_progress = {"current_page": 0, "total_pages": total_pages, "completed_pages": 0}
            self.log(f"🚀 [START BOOK] {filename} | Total Pages: {total_pages}")

            pages_tasks = []
            for idx in range(total_pages):
                pno = idx + 1
                page_json_path = os.path.join(pages_dir, f"p_{pno:04d}.json")
                page = doc[idx]
                text = page.get_text("text").strip()
                imgs = page.get_images()
                if len(imgs) == 0 and len(text) > 100 and not any(ord(c) == 0xFFFD for c in text):
                    pages_tasks.append((idx, pno, total_pages, "digital_text", text, page_json_path))
                else:
                    pw, ph = float(page.rect.width), float(page.rect.height)
                    scale = min(1.8, 1280.0 / max(pw, ph, 1.0))
                    pix = page.get_pixmap(matrix=pymupdf.Matrix(scale, scale))
                    b64_img = base64.b64encode(pix.tobytes("jpeg", jpg_quality=85)).decode("ascii")

                    scale_slice = min(2.0, 1280.0 / max(pw, ph * 0.54, 1.0))
                    pix_top = page.get_pixmap(clip=pymupdf.Rect(0, 0, pw, ph * 0.54), matrix=pymupdf.Matrix(scale_slice, scale_slice))
                    b64_top = base64.b64encode(pix_top.tobytes("jpeg", jpg_quality=85)).decode("ascii")

                    pix_bot = page.get_pixmap(clip=pymupdf.Rect(0, ph * 0.46, pw, ph), matrix=pymupdf.Matrix(scale_slice, scale_slice))
                    b64_bot = base64.b64encode(pix_bot.tobytes("jpeg", jpg_quality=85)).decode("ascii")

                    payload = {"full": b64_img, "top": b64_top, "bot": b64_bot}
                    pages_tasks.append((idx, pno, total_pages, "vision", payload, page_json_path))

            doc.close()
            t_start = time.time()
            pages_data = [None] * total_pages

            total_workers = sentinel.get_cluster_snapshot()["total_active_slots"] or 6
            with concurrent.futures.ThreadPoolExecutor(max_workers=max(4, total_workers)) as ex:
                futs = {
                    ex.submit(self.parse_page_task, idx, pno, tot, itype, pdata, ppath): idx
                    for (idx, pno, tot, itype, pdata, ppath) in pages_tasks
                }
                for fut in concurrent.futures.as_completed(futs):
                    idx = futs[fut]
                    pages_data[idx] = fut.result()

            elapsed_total = round(time.time() - t_start, 1)

            # Strict 100% clean verification gate
            has_errors = any((not p or p.get("raw_text", "").startswith("ERROR:")) for p in pages_data)
            if has_errors:
                err_count = sum(1 for p in pages_data if (not p or p.get("raw_text", "").startswith("ERROR:")))
                self.log(f"❌ [BOOK FAILED VERIFICATION] {filename} has {err_count}/{total_pages} error pages! Skipping book.json.", "ERROR")
                return False

            # Assemble clean book.json
            book_json_data = {
                "drive_file_id": fid,
                "filename": filename,
                "collection": col,
                "book_slug": book_slug,
                "subject": item.get("subject"),
                "edition": item.get("edition"),
                "total_pages": total_pages,
                "completed_pages": total_pages,
                "elapsed_seconds": elapsed_total,
                "parsed_at": datetime.datetime.now().isoformat(),
                "pages": pages_data,
            }

            tmp_book = book_json_path + ".tmp"
            with open(tmp_book, "w", encoding="utf-8") as f:
                json.dump(book_json_data, f, ensure_ascii=False, indent=2)
            os.replace(tmp_book, book_json_path)

            if tracker and tracker.enabled:
                tracker.mark_completed(fid, total_pages=total_pages, completed_pages=total_pages, output_json=book_json_path)

            self.stats["completed_books"] += 1
            self.stats["total_pages_parsed"] += total_pages
            self.log(f"🎉 [BOOK COMPLETED] {filename} | {total_pages} pages in {elapsed_total}s -> {book_json_path}", "SUCCESS")
            return True

        finally:
            cleanup_temp_file(temp_pdf)

    def run_worker_loop(self):
        self.log("🚀 Orchestrator worker thread initialized. Running continuous 24/7 parsing loop...")
        
        while self.running:
            tracker = get_tracker()
            if tracker.enabled:
                try:
                    tracker.refresh_cache(force=True)
                except Exception as e:
                    self.log(f"CloudTracker sync warning: {e}", "WARN")

            if not os.path.exists(MANIFEST_FILE):
                self.log("❌ manifest.json not found! Exiting worker loop.", "ERROR")
                break

            with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
                manifest_data = json.load(f)

            items = manifest_data.get("items", [])
            
            # Filter pending books that are not yet marked COMPLETED
            pending_items = []
            for it in items:
                fid = it.get("drive_file_id")
                if tracker and tracker.enabled and tracker.is_already_completed(fid):
                    continue
                # Also check local disk
                col = it.get("collection", "unspecified")
                book_slug = it.get("book_slug", fid)
                book_json_path = os.path.join(PARSED_DIR, col, book_slug, "book.json")
                if os.path.exists(book_json_path):
                    continue
                pending_items.append(it)

            self.log(f"📚 Status: {len(pending_items)} pending books remaining out of {len(items)} catalog.")

            if len(pending_items) == 0:
                self.log("🎉🎉 [MISSION COMPLETE] All 1,496 books in catalog have been parsed and verified clean!", "SUCCESS")
                time.sleep(60)
                continue

            for item in pending_items:
                while self.paused:
                    time.sleep(2)
                if not self.running:
                    break
                try:
                    self.process_book(item, tracker)
                except Exception as e:
                    self.log(f"Unexpected error on {item.get('filename')}: {e}", "ERROR")
                    time.sleep(2)

    def start(self):
        if self.running:
            return
        self.running = True
        self._thread = threading.Thread(target=self.run_worker_loop, daemon=True)
        self._thread.start()

    def pause(self):
        self.paused = True
        self.log("⏸️ Orchestrator paused by user.")

    def resume(self):
        self.paused = False
        self.log("▶️ Orchestrator resumed.")


orchestrator = ParsingOrchestrator()
