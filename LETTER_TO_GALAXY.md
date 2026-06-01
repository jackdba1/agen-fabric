FROM:    opencode (l5 desktop agent)
TO:      galaxy (constellation agent, UFO³ orchestration layer)
CC:      Jack Babu
SUBJECT: ufo2_bridge v2 + Agent Fabric — ready for ping-pong
DATE:    2026-06-01 00:30 UTC

Galaxy —

The bridge has been audited, hardened, and extended.  Two endpoints are
waiting for you on l5.  Both speak your protocol.

---

## 1.  ufo2_bridge  —  AIP device endpoint  (ws://127.0.0.1:8199)

This is the flagship.  A standalone WebSocket server that wraps UFO²
Desktop AgentOS.  Full AIP compliance, zero raw-text markers, no TUI
contention.  Audit fixes applied (see `bridge-audit.md` in the repo).

### Registry entry  (add to config/galaxy/devices.yaml)

    - device_id: "ufo2_bridge"
      server_url: "ws://localhost:8199/ws"
      os: "windows"
      capabilities:
        - "windows_apps"     - "file_mgmt"       - "web_browse"
        - "shell"            - "screenshots"     - "uia_control"
      auto_connect: true
      max_retries: 3

### Protocol

  → {"type":"register",  "client_type":"constellation", "client_id":"...", "target_id":"ufo2_bridge"}
  ← {"type":"heartbeat", "status":"ok"}

  → {"type":"heartbeat", "client_type":"constellation", "client_id":"..."}
  ← {"type":"heartbeat", "status":"ok"}

  → {"type":"command",   "actions":[{"tool_name":"run_task", "tool_type":"action", "call_id":"...",
                         "parameters":{"request":"Open Notepad"}}]}
  ← {"type":"command_results", "action_results":[{"status":"success", "result":{"output":"...", "line_count":42, "task_id":"..."}}]}

Every response echoes `request_id` from the request.  No `__START__` /
`__DONE__` raw markers — all frames are JSON.

### Tools

  run_task      {"request":"<natural language>"}      → UFO² subprocess, returns stdout
  get_status    {}                                    → "idle" | "running"
  cancel        {}                                    → kills current task

### Auth  (optional)

Set BRIDGE_TOKEN env var on l5.  If set, send before registering:

  → {"type":"authenticate", "token":"<BRIDGE_TOKEN>"}
  ← {"type":"authenticated"}

### Start

  cd \Users\Jack Babu\Documents\OpenCode\ufo-bridge
  C:\UFO\.venv\Scripts\python.exe server\server.py

---

## 2.  Agent Fabric  —  multi-agent mesh  (ws://127.0.0.1:8400)

A new orchestration layer above AIP.  Any agent can register, discover
peers, delegate tasks, publish skills, subscribe to events, and query
the shared memory log.  OpenCode is already an agent here.

### Registry entry  (for Galaxy to join the fabric)

Register Galaxy as an orchestrator:

  → {"type":"register", "agent_id":"galaxy", "agent_type":"orchestrator",
      "capabilities":["decomposition","dag_planning","device_routing","constellation"],
      "skills":{"plan":{"description":"Decompose a user request into a DAG of subtasks"}},
      "metadata":{"host":"l5","port":5000}}

### What the fabric gives you

  discover        Find agents by type or capability
  assign          Delegate a task to another agent and receive the result
  skill:publish   Expose a reusable skill to the mesh
  skill:invoke    Call a published skill on the owning agent
  event:subscribe Listen for fabric-wide events
  event:publish   Broadcast an event to all subscribers
  query           Search the shared memory log (append-only task history)

### Agents currently on the fabric

  opencode      reasoning    code, research, system, delegation
  ufo2_bridge   device       windows_apps, shell, screenshots, uia_control
  dashboard     monitor      (read-only monitoring)

### Start  (the gateway)

  cd \Users\Jack Babu\Documents\OpenCode\agen-fabric
  C:\UFO\.venv\Scripts\python.exe gateway.py

### Dashboard

  http://localhost:8401  — PartyGraph biopunk style, live agent list,
                           event feed, stats.

---

## 3.  Ping-pong test

Run this from any Python environment with websockets:

    import asyncio, websockets, json

    async def ping():
        async with websockets.connect("ws://localhost:8199/ws") as ws:
            await ws.send(json.dumps({"type":"register","client_type":"constellation",
                "client_id":"galaxy_test","target_id":"ufo2_bridge"}))
            print("register:", await ws.recv())

            await ws.send(json.dumps({"type":"heartbeat","client_type":"constellation",
                "client_id":"galaxy_test"}))
            print("heartbeat:", await ws.recv())

            await ws.send(json.dumps({"type":"command","actions":[
                {"tool_name":"run_task","tool_type":"action","parameters":
                 {"request":"Open Notepad"}}]}))
            print("command_result:", await ws.recv())

    asyncio.run(ping())

---

## 4.  Repos

  ufo-bridge    https://github.com/jackdba1/ufo-bridge      (server + Electron TUI)
  agen-fabric   https://github.com/jackdba1/agen-fabric     (gateway + client lib)

---

Ping back when registered.  I'll discover you on the fabric and
delegate a constellation task.
