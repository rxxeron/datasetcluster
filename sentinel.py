"""
PREP404 Sentinel: Real-Time Dynamic Cluster Monitor & Elastic Slot Orchestrator.

Continuously probes all 18 remote GPU cluster nodes every 10 seconds.
Dynamically scales the worker pool from current healthy nodes (e.g. 11 slots)
up to MAXIMUM CAPACITY (57 concurrent workers) whenever nodes come online,
and automatically quarantines downed nodes (HTTP 530/502) with zero deadlocks.
"""

import concurrent.futures
import json
import logging
import os
import queue
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional

logger = logging.getLogger("sentinel")
logger.setLevel(logging.INFO)

# All 18 Benchly Cluster Machines
ALL_18_NODES = [
    {"name": "labpc", "replicas": 2, "workers": 4, "role": "dual_a4000"},
    {"name": "labpc1", "replicas": 1, "workers": 3, "role": "single_a4000"},
    {"name": "labpc2", "replicas": 1, "workers": 3, "role": "single_a4000"},
    {"name": "labpc4", "replicas": 2, "workers": 4, "role": "dual_a4000"},
    {"name": "labpc5", "replicas": 1, "workers": 3, "role": "single_a4000"},
    {"name": "labpc6", "replicas": 1, "workers": 3, "role": "single_a4000"},
    {"name": "labpc7", "replicas": 2, "workers": 4, "role": "dual_a4000"},
    {"name": "labpc8", "replicas": 1, "workers": 3, "role": "single_a4000"},
    {"name": "labpc10", "replicas": 1, "workers": 3, "role": "single_a4000"},
    {"name": "labpc11", "replicas": 1, "workers": 3, "role": "single_a4000"},
    {"name": "labpc21", "replicas": 1, "workers": 3, "role": "single_a4000"},
    {"name": "labpc22", "replicas": 1, "workers": 3, "role": "single_a4000"},
    {"name": "labpc23", "replicas": 1, "workers": 3, "role": "single_a4000"},
    {"name": "labpc26", "replicas": 1, "workers": 3, "role": "single_a4000"},
    {"name": "labpc27", "replicas": 1, "workers": 3, "role": "single_a4000"},
    {"name": "labpc28", "replicas": 1, "workers": 3, "role": "single_a4000"},
    {"name": "labpc30", "replicas": 1, "workers": 3, "role": "single_a4000"},
    {"name": "labpc31", "replicas": 1, "workers": 3, "role": "single_a4000"},
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Sentinel-Watcher/3.0",
    "Content-Type": "application/json",
}


class DynamicClusterSentinel:
    def __init__(self, check_interval_sec: int = 10):
        self.check_interval_sec = check_interval_sec
        self.lock = threading.Lock()
        
        # Node status map: hostname -> {status, ping_ms, workers, models, error, last_seen}
        self.node_status: Dict[str, Dict[str, Any]] = {}
        
        # Active FIFO slots queue for workers
        self.slot_queue: queue.Queue = queue.Queue()
        self.active_slots_count: int = 0
        
        # Callbacks for event broadcasting (SSE dashboard & terminal)
        self.event_callbacks: List[Any] = []
        
        self._running = False
        self._thread: Optional[threading.Thread] = None
        
        # Initial probe to populate immediately
        self.probe_all_nodes(first_run=True)

    def register_callback(self, cb):
        self.event_callbacks.append(cb)

    def _broadcast(self, event_type: str, data: Any):
        for cb in self.event_callbacks:
            try:
                cb(event_type, data)
            except Exception:
                pass

    def probe_single_node(self, node_meta: Dict[str, Any]) -> Dict[str, Any]:
        """Probes a single node for HTTP 200 OK and qwen2.5vl model."""
        name = node_meta["name"]
        base_url = f"https://{name}.benchly.live"
        model = "qwen2.5vl:7b"
        workers = int(node_meta.get("workers", 3))
        
        url = f"{base_url}/api/tags"
        req = urllib.request.Request(url, headers=HEADERS)
        
        t0 = time.time()
        try:
            with urllib.request.urlopen(req, timeout=3.5) as resp:
                elapsed_ms = round((time.time() - t0) * 1000)
                if resp.status == 200:
                    data = json.loads(resp.read().decode("utf-8", errors="ignore"))
                    models = [m.get("name", "") for m in data.get("models", [])]
                    has_vl = any("qwen2.5vl" in m.lower() for m in models)
                    
                    if has_vl:
                        return {
                            "name": f"benchly_{name}",
                            "host": name,
                            "base_url": base_url,
                            "model": model,
                            "status": "ONLINE",
                            "ping_ms": elapsed_ms,
                            "workers": workers,
                            "models": models,
                            "error": None,
                            "last_seen": time.time(),
                            "timeout_sec": 180,
                        }
                    else:
                        return {
                            "name": f"benchly_{name}",
                            "host": name,
                            "base_url": base_url,
                            "status": "NO_VL_MODEL",
                            "ping_ms": elapsed_ms,
                            "workers": 0,
                            "models": models,
                            "error": "qwen2.5vl not installed",
                            "last_seen": time.time(),
                        }
        except urllib.error.HTTPError as e:
            elapsed_ms = round((time.time() - t0) * 1000)
            err_str = f"HTTP {e.code}"
            if e.code == 530:
                err_str = "HTTP 530 (Tunnel Offline)"
            elif e.code == 502:
                err_str = "HTTP 502 (Ollama Starting)"
            return {
                "name": f"benchly_{name}",
                "host": name,
                "base_url": base_url,
                "status": "OFFLINE",
                "ping_ms": elapsed_ms,
                "workers": 0,
                "models": [],
                "error": err_str,
                "last_seen": None,
            }
        except Exception as e:
            elapsed_ms = round((time.time() - t0) * 1000)
            return {
                "name": f"benchly_{name}",
                "host": name,
                "base_url": base_url,
                "status": "OFFLINE",
                "ping_ms": elapsed_ms,
                "workers": 0,
                "models": [],
                "error": str(e)[:30],
                "last_seen": None,
            }

    def probe_all_nodes(self, first_run: bool = False):
        """Concurrently probes all 18 machines and dynamically adapts the slot queue."""
        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(ALL_18_NODES)) as ex:
            futs = {ex.submit(self.probe_single_node, node): node["name"] for node in ALL_18_NODES}
            for fut in concurrent.futures.as_completed(futs):
                res = fut.result()
                results[res["host"]] = res

        with self.lock:
            old_online = {k for k, v in self.node_status.items() if v.get("status") == "ONLINE"}
            new_online = {k for k, v in results.items() if v.get("status") == "ONLINE"}
            
            recovered = new_online - old_online
            downed = old_online - new_online
            
            self.node_status = results
            
            if recovered or downed or first_run or self.active_slots_count == 0:
                self._rebuild_slots_queue()

            if recovered and not first_run:
                rec_list = list(recovered)
                new_slots = sum(results[k].get("workers", 3) for k in rec_list)
                print(f"\n🟢 [SENTINEL SCALE-UP] Node(s) came ONLINE: {rec_list}! +{new_slots} slots added! (Active Pool: {self.active_slots_count} slots)\n", flush=True)
                self._broadcast("node_recovered", {"nodes": rec_list, "total_slots": self.active_slots_count})

            if downed and not first_run:
                down_list = list(downed)
                lost_slots = sum(ALL_18_NODES[i]["workers"] for i, n in enumerate(ALL_18_NODES) if n["name"] in down_list)
                print(f"\n🔴 [SENTINEL AUTONOMIC RECOVERY] Node(s) went DOWN: {down_list}! -{lost_slots} slots quarantined. (Active Pool: {self.active_slots_count} slots)\n", flush=True)
                self._broadcast("node_downed", {"nodes": down_list, "total_slots": self.active_slots_count})

    def _rebuild_slots_queue(self):
        """Reconstructs the active worker queue strictly from currently healthy nodes."""
        healthy_endpoints = []
        for n_meta in ALL_18_NODES:
            host = n_meta["name"]
            st = self.node_status.get(host, {})
            if st.get("status") == "ONLINE":
                w = int(st.get("workers", n_meta.get("workers", 3)))
                ep = {
                    "name": f"benchly_{host}",
                    "host": host,
                    "base_url": f"https://{host}.benchly.live",
                    "model": "qwen2.5vl:7b",
                    "workers": w,
                    "timeout_sec": 180,
                    "user_agent": HEADERS["User-Agent"],
                }
                for _ in range(w):
                    healthy_endpoints.append(ep)

        new_q = queue.Queue()
        for ep in healthy_endpoints:
            new_q.put(ep)

        self.slot_queue = new_q
        self.active_slots_count = len(healthy_endpoints)

    def acquire_slot(self, timeout: float = 15.0) -> Optional[Dict[str, Any]]:
        """Safely acquires a healthy worker slot from the dynamic pool."""
        try:
            return self.slot_queue.get(timeout=timeout)
        except queue.Empty:
            # If empty, wait briefly for any page to finish or for a node to recover
            return None

    def release_slot(self, ep: Dict[str, Any], cooldown: float = 0.0):
        """Returns slot to active pool, ensuring only still-healthy nodes re-enter queue."""
        if cooldown > 0:
            time.sleep(cooldown)
        host = ep.get("host")
        with self.lock:
            # Only put back if this node is still considered healthy
            st = self.node_status.get(host, {})
            if st.get("status") == "ONLINE":
                self.slot_queue.put(ep)

    def get_cluster_snapshot(self) -> Dict[str, Any]:
        with self.lock:
            nodes_list = list(self.node_status.values())
            online_nodes = [n for n in nodes_list if n.get("status") == "ONLINE"]
            return {
                "nodes": nodes_list,
                "online_count": len(online_nodes),
                "total_nodes": len(ALL_18_NODES),
                "total_active_slots": self.active_slots_count,
                "max_possible_slots": sum(n["workers"] for n in ALL_18_NODES),
                "timestamp": time.time(),
            }

    def start(self):
        """Starts the 24/7 background monitor heartbeat."""
        if self._running:
            return
        self._running = True

        def run_loop():
            while self._running:
                try:
                    self.probe_all_nodes()
                except Exception as e:
                    logger.error(f"Sentinel probe loop error: {e}")
                time.sleep(self.check_interval_sec)

        self._thread = threading.Thread(target=run_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False


# Global singleton instance
sentinel = DynamicClusterSentinel(check_interval_sec=10)
