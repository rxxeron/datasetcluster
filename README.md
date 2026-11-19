---
title: PREP404 Datacenter
emoji: 🛰️
colorFrom: cyan
colorTo: blue
sdk: docker
app_port: 7860
pinned: false
---

# 🛰️ PREP404 Datacenter — 24/7 Cluster Orchestrator & Live Monitor

Autonomous 24/7 self-healing parser engine and real-time dashboard for Bangladeshi academic textbooks.

### Core Architecture
- **Sentinel Daemon**: Continuously probes remote NVIDIA RTX A4000 GPU nodes on `*.benchly.live`.
- **Dynamic Slot Auto-Healing**: Automatically quarantines offline nodes (e.g. HTTP 530) and instantly admits recovered nodes without stopping the pipeline.
- **Adaptive Dense Page Slicing**: Automatically splits dense pages (>2,350 chars) into Top/Bottom overlapping halves and merges clean output.
- **Strict Verification Gate**: 100% clean guarantee (blocks incomplete or error-tagged pages from `book.json`).
- **Live Real-time Web Dashboard**: Server-Sent Events (SSE) streaming node health, active book progress, and live terminal logs.
- **Download Completed Books**: Direct one-click `.zip` export of all verified books.
