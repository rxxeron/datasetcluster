"""
PREP404 Datacenter Dashboard & API Server.
FastAPI + Server-Sent Events (SSE) real-time cluster monitor,
self-healing coordinator controls, and catalog download manager.
"""

import asyncio
import io
import json
import os
import time
import zipfile
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from orchestrator import PARSED_DIR, orchestrator
from sentinel import sentinel

app = FastAPI(title="PREP404 Datacenter - Vision OCR Cluster & Cloud Orchestrator")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

# Real-time event broadcasting queue
event_queue = asyncio.Queue()


def sse_event_listener(event_type: str, data: any):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.call_soon_threadsafe(event_queue.put_nowait, {"event": event_type, "data": data})
    except Exception:
        pass


sentinel.register_callback(lambda et, d: sse_event_listener(et, d))
orchestrator.add_log_listener(lambda entry: sse_event_listener("log", entry))


@app.on_event("startup")
def startup_event():
    # Start sentinel watchdog and parser orchestrator
    sentinel.start()
    orchestrator.start()


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/status")
async def get_status():
    cluster_snap = sentinel.get_cluster_snapshot()
    return {
        "cluster": cluster_snap,
        "current_book": orchestrator.current_book,
        "book_progress": orchestrator.book_progress,
        "stats": orchestrator.stats,
        "paused": orchestrator.paused,
        "uptime_sec": round(time.time() - orchestrator.stats["start_time"]),
    }


@app.get("/api/stream")
async def sse_stream(request: Request):
    """Server-Sent Events streaming live cluster events to the dashboard in real-time."""
    async def event_generator():
        while True:
            if await request.is_disconnected():
                break
            try:
                # Wait for next event or send periodic keepalive
                event_data = await asyncio.wait_for(event_queue.get(), timeout=12.0)
                yield f"event: {event_data['event']}\ndata: {json.dumps(event_data['data'])}\n\n"
            except asyncio.TimeoutError:
                # Keep-alive heartbeat
                yield f": heartbeat\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.post("/api/control/{action}")
async def control_action(action: str, request: Request):
    data = await request.json() if request.headers.get("content-type") == "application/json" else {}
    if action == "pause":
        orchestrator.pause()
        return {"status": "paused"}
    elif action == "resume":
        orchestrator.resume()
        return {"status": "resumed"}
    elif action == "update_node":
        node_name = data.get("name")
        workers = data.get("workers")
        enabled = data.get("enabled")
        for ep in sentinel.endpoint_configs:
            if ep["name"] == node_name:
                if workers is not None:
                    ep["workers"] = int(workers)
                if enabled is not None:
                    ep["enabled"] = bool(enabled)
                break
        sentinel.save_configs()
        sentinel._rebuild_slot_pool()
        return {"status": "updated", "node": node_name}
    raise HTTPException(status_code=400, detail="Unknown action")


@app.get("/api/download_zip")
async def download_all_zip():
    """Compress and stream all completed book.json files into a single zip file."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(PARSED_DIR):
            for file in files:
                if file == "book.json":
                    full_path = os.path.join(root, file)
                    rel_path = os.path.relpath(full_path, PARSED_DIR)
                    zf.write(full_path, arcname=rel_path)
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=prep404_datacenter_completed_books.zip"},
    )


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 7860))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
