"""
AAPathshala Sentinel: Self-Healing Cluster Health & Slot Orchestrator.
Continuously monitors all remote GPU nodes, auto-heals downed endpoints,
and dynamically manages worker slot allocation without deadlocks or crashes.
"""

import concurrent.futures
import json
import logging
import os
import queue
import threading
import time
import urllib.request
from typing import Any, Dict, List, Optional

logger = logging.getLogger("sentinel")
logger.setLevel(logging.INFO)

ENDPOINTS_FILE = os.path.join(os.path.dirname(__file__), "endpoints.json")


class ClusterSentinel:
    def __init__(self, endpoints_path: str = ENDPOINTS_FILE, check_interval_sec: int = 10):
        self.endpoints_path = endpoints_path
        self.check_interval_sec = check_interval_sec
        self.lock = threading.Lock()
        
        # Raw endpoint configurations
        self.endpoint_configs: List[Dict[str, Any]] = self._load_configs()
        
        # Live node status: name -> {status, ping_ms, workers, models, error, last_seen}
        self.node_status: Dict[str, Dict[str, Any]] = {}
        
        # Active FIFO slots queue for workers
        self.slot_queue: queue.Queue = queue.Queue()
        self._current_slot_endpoints: List[Dict[str, Any]] = []
        
        # Callback for event streaming (to dashboard SSE)
        self.event_callbacks: List[Any] = []
        
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def _load_configs(self) -> List[Dict[str, Any]]:
        if os.path.exists(self.endpoints_path):
            try:
                with open(self.endpoints_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading endpoints.json: {e}")
        return []

    def save_configs(self):
        with self.lock:
            with open(self.endpoints_path, "w", encoding="utf-8") as f:
                json.dump(self.endpoint_configs, f, indent=2, ensure_ascii=False)

    def register_callback(self, cb):
        self.event_callbacks.append(cb)

    def _broadcast(self, event_type: str, data: Any):
        for cb in self.event_callbacks:
            try:
                cb(event_type, data)
            except Exception:
                pass

    def probe_node(self, ep: Dict[str, Any]) -> Dict[str, Any]:
        """Send a lightweight ping to Ollama /api/tags."""
        name = ep["name"]
        base_url = ep["base_url"].rstrip("/")
        model = ep.get("model", "qwen2.5vl:7b")
        workers = int(ep.get("workers", 3))
        enabled = ep.get("enabled", True)

        if not enabled:
            return {
                "name": name,
                "status": "DISABLED",
                "ping_ms": None,
                "workers": 0,
                "models": [],
                "error": "Disabled by user",
                "last_seen": None,
            }

        url = f"{base_url}/api/tags"
        headers = {"User-Agent": "Mozilla/5.0 Sentinel/2.0"}
        req = urllib.request.Request(url, headers=headers)
        
        t0 = time.time()
        try:
            with urllib.request.urlopen(req, timeout=4) as resp:
                elapsed_ms = round((time.time() - t0) * 1000)
                if resp.status == 200:
                    data = json.loads(resp.read().decode("utf-8", errors="ignore"))
                    models = [m.get("name", "") for m in data.get("models", [])]
                    return {
                        "name": name,
                        "base_url": base_url,
                        "model": model,
                        "status": "ONLINE",
                        "ping_ms": elapsed_ms,
                        "workers": workers,
                        "models": models,
                        "error": None,
                        "last_seen": time.time(),
                        "timeout_sec": int(ep.get("timeout_sec", 180)),
                    }
                else:
                    return {
                        "name": name,
                        "base_url": base_url,
                        "status": "ERROR",
                        "ping_ms": elapsed_ms,
                        "workers": 0,
                        "models": [],
                        "error": f"HTTP {resp.status}",
                        "last_seen": None,
                    }
        except Exception as e:
            elapsed_ms = round((time.time() - t0) * 1000)
            err_str = str(e)
            if "530" in err_str:
                err_clean = "HTTP 530 (Tunnel Offline)"
            elif "timed out" in err_str:
                err_clean = "Timeout (>4s)"
            else:
                err_clean = err_str[:35]
            return {
                "name": name,
                "base_url": base_url,
                "status": "OFFLINE",
                "ping_ms": elapsed_ms,
                "workers": 0,
                "models": [],
                "error": err_clean,
                "last_seen": None,
            }

    def check_all_nodes(self):
        """Probes all configured nodes concurrently and updates the slot pool."""
        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.endpoint_configs) or 1) as ex:
            futs = {ex.submit(self.probe_node, ep): ep["name"] for ep in self.endpoint_configs}
            for fut in concurrent.futures.as_completed(futs):
                res = fut.result()
                results[res["name"]] = res

        with self.lock:
            old_online = {k for k, v in self.node_status.items() if v.get("status") == "ONLINE"}
            new_online = {k for k, v in results.items() if v.get("status") == "ONLINE"}
            
            # Detect recovered or downed nodes
            recovered = new_online - old_online
            downed = old_online - new_online
            
            self.node_status = results
            
            if recovered or downed or not self._current_slot_endpoints:
                self._rebuild_slot_pool()

            if recovered:
                self._broadcast("node_recovered", {"nodes": list(recovered)})
            if downed:
                self._broadcast("node_downed", {"nodes": list(downed)})

        self._broadcast("cluster_update", self.get_cluster_snapshot())

    def _rebuild_slot_pool(self):
        """Reconstructs the active worker slot queue based strictly on healthy ONLINE nodes."""
        healthy_endpoints = []
        for ep in self.endpoint_configs:
            name = ep["name"]
            st = self.node_status.get(name, {})
            if st.get("status") == "ONLINE":
                w = int(ep.get("workers", 3))
                healthy_ep = dict(ep)
                healthy_ep["workers"] = w
                healthy_ep["timeout_sec"] = int(ep.get("timeout_sec", 180))
                for _ in range(w):
                    healthy_endpoints.append(healthy_ep)

        # Clear and refill queue
        new_q = queue.Queue()
        for item in healthy_endpoints:
            new_q.put(item)
            
        self.slot_queue = new_q
        self._current_slot_endpoints = healthy_endpoints

    def acquire_slot(self, timeout: float = 25.0) -> Optional[Dict[str, Any]]:
        """Safely acquire a slot from the healthy queue."""
        try:
            return self.slot_queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def release_slot(self, ep: Dict[str, Any], cooldown: float = 0.0):
        """Return slot to queue, optionally with cooldown if a temporary network hiccup occurred."""
        if cooldown > 0:
            time.sleep(cooldown)
        self.slot_queue.put(ep)

    def get_cluster_snapshot(self) -> Dict[str, Any]:
        with self.lock:
            nodes_list = list(self.node_status.values())
            online_count = sum(1 for n in nodes_list if n.get("status") == "ONLINE")
            total_slots = sum(n.get("workers", 0) for n in nodes_list if n.get("status") == "ONLINE")
            return {
                "nodes": nodes_list,
                "online_count": online_count,
                "total_nodes": len(nodes_list),
                "total_active_slots": total_slots,
                "timestamp": time.time(),
            }

    def start(self):
        """Starts the background monitoring loop."""
        if self._running:
            return
        self._running = True
        
        def run_loop():
            logger.info("Sentinel watchdog loop started.")
            while self._running:
                try:
                    self.check_all_nodes()
                except Exception as e:
                    logger.error(f"Sentinel check error: {e}")
                time.sleep(self.check_interval_sec)

        self._thread = threading.Thread(target=run_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False


# Global Sentinel instance
sentinel = ClusterSentinel()
