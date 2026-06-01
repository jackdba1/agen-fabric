#!/usr/bin/env python3
"""
Agent Fabric Gateway — central hub for multi-agent orchestration.

All agents connect here via WebSocket. The gateway provides:
  - Agent registry (register, heartbeat, discover, de-register on disconnect)
  - Task routing (find capable agent, delegate, await result, return)
  - Skill marketplace (publish skills, invoke by name)
  - Event bus (pub/sub for fabric-wide events)
  - Memory log (append-only history of all tasks and their outcomes)
  - Monitoring dashboard (HTTP + WebSocket for live topology view)

Protocol: JSON over WebSocket, extends AIP message envelope.
"""

import os, sys, re, json, uuid, time, asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, List, Any, Set
from dataclasses import dataclass, field
import websockets

# ── Config ──
GATEWAY_PORT = int(os.environ.get("FABRIC_PORT", "8400"))
HTTP_PORT    = GATEWAY_PORT + 1

# ── Helpers ──
def _id() -> str:   return uuid.uuid4().hex[:8]
def _ts() -> str:   return datetime.now(timezone.utc).isoformat()
def _now() -> float: return time.time()

# ── Agent model ──

@dataclass
class Agent:
    agent_id: str
    agent_type: str           # "reasoning" | "device" | "orchestrator" | "memory"
    capabilities: List[str] = field(default_factory=list)
    skills: Dict[str, dict] = field(default_factory=dict)
    status: str = "online"
    last_heartbeat: float = field(default_factory=_now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    websocket: Optional[Any] = None  # websockets connection for direct messaging

# ── Gateway state ──

class FabricState:
    def __init__(self):
        self.agents: Dict[str, Agent] = {}
        self.events: List[dict] = []       # append-only event log
        self.active_tasks: Dict[str, dict] = {}  # task_id -> task tracking
        self.subscribers: Dict[str, Set[Any]] = {}  # event_type -> set of websockets
        self.start_time = _now()

state = FabricState()

# ── Message builders ──

def ok_msg(msg_type: str, **fields) -> dict:
    return {"type": msg_type, "status": "ok", "timestamp": _ts(), "response_id": _id()} | fields

def err_msg(reason: str) -> dict:
    return {"type": "error", "status": "error", "error": reason, "timestamp": _ts()}

# ── Gateway handlers ──

async def handle_register(ws, msg: dict) -> dict:
    aid = msg.get("agent_id") or _id()
    atype = msg.get("agent_type", "device")
    caps = msg.get("capabilities", [])
    skills = msg.get("skills", {})
    meta = msg.get("metadata", {})

    if aid in state.agents and state.agents[aid].websocket:
        # Re-registration — close old connection
        old = state.agents[aid]
        try: await old.websocket.close()
        except: pass

    agent = Agent(
        agent_id=aid, agent_type=atype, capabilities=caps,
        skills=skills, metadata=meta, websocket=ws,
    )
    state.agents[aid] = agent

    # Broadcast fabric event
    await broadcast_event("agent.registered", {
        "agent_id": aid, "agent_type": atype,
        "capabilities": caps, "skill_count": len(skills),
    })

    return ok_msg("registered", agent_id=aid, skill_count=len(skills))

async def handle_heartbeat(ws, msg: dict) -> dict:
    aid = msg.get("agent_id")
    if aid and aid in state.agents:
        state.agents[aid].last_heartbeat = _now()
        state.agents[aid].status = "online"
    return ok_msg("heartbeat")

async def handle_discover(ws, msg: dict) -> dict:
    """Find agents matching given criteria."""
    atype = msg.get("agent_type")
    capability = msg.get("capability")
    results = []
    for agent in state.agents.values():
        if atype and agent.agent_type != atype:
            continue
        if capability and capability not in agent.capabilities:
            continue
        results.append({
            "agent_id": agent.agent_id,
            "agent_type": agent.agent_type,
            "capabilities": agent.capabilities,
            "skills": list(agent.skills.keys()),
            "status": agent.status,
            "metadata": agent.metadata,
        })
    return ok_msg("discover", agents=results, count=len(results))

async def handle_assign(ws, msg: dict) -> dict:
    """Route a task to a capable agent."""
    target_id = msg.get("target_id")
    task = msg.get("task", {})
    task_id = _id()

    if target_id not in state.agents:
        return err_msg(f"Agent '{target_id}' not found")

    target = state.agents[target_id]
    if not target.websocket:
        return err_msg(f"Agent '{target_id}' is not connected")

    # Track the task
    state.active_tasks[task_id] = {
        "task_id": task_id, "from": msg.get("agent_id", "unknown"),
        "to": target_id, "task": task, "status": "pending",
        "started": _ts(),
    }

    # Forward to target
    envelope = ok_msg("task", task_id=task_id, from_agent=msg.get("agent_id"), **task)
    try:
        await target.websocket.send(json.dumps(envelope))
    except Exception as e:
        state.active_tasks[task_id]["status"] = "failed"
        return err_msg(f"Failed to send to {target_id}: {e}")

    # Wait for result (blocking — target must respond on same connection)
    try:
        raw = await asyncio.wait_for(target.websocket.recv(), timeout=120)
        result = json.loads(raw)
        if result.get("task_id") == task_id and result.get("type") == "task_result":
            state.active_tasks[task_id]["status"] = "completed"
            state.active_tasks[task_id]["result"] = result.get("result")
            # Log to event history
            await log_event("task.completed", {
                "task_id": task_id, "from": msg.get("agent_id"),
                "to": target_id, "result": result.get("result"),
            })
            return ok_msg("assign", task_id=task_id, status="completed", result=result.get("result"))
    except asyncio.TimeoutError:
        state.active_tasks[task_id]["status"] = "timeout"
        return err_msg(f"Task {task_id} timed out waiting for {target_id}")

    return err_msg(f"Unexpected response from {target_id}")

async def handle_skill_publish(ws, msg: dict) -> dict:
    aid = msg.get("agent_id")
    skill_name = msg.get("skill_name")
    skill_def = msg.get("skill", {})
    if aid in state.agents:
        state.agents[aid].skills[skill_name] = skill_def
        await broadcast_event("skill.published", {
            "agent_id": aid, "skill_name": skill_name,
            "description": skill_def.get("description", ""),
        })
        return ok_msg("skill_published", skill_name=skill_name)
    return err_msg("Agent not registered")

async def handle_skill_invoke(ws, msg: dict) -> dict:
    """Invoke a skill by routing to the agent that owns it."""
    skill_name = msg.get("skill_name")
    params = msg.get("parameters", {})
    for agent in state.agents.values():
        if skill_name in agent.skills and agent.websocket:
            envelope = ok_msg("skill_invoke", skill_name=skill_name, parameters=params, task_id=_id())
            await agent.websocket.send(json.dumps(envelope))
            try:
                raw = await asyncio.wait_for(agent.websocket.recv(), timeout=60)
                result = json.loads(raw)
                if result.get("type") == "skill_result":
                    return ok_msg("skill_invoked", skill_name=skill_name, result=result.get("result"))
            except asyncio.TimeoutError:
                pass
    return err_msg(f"Skill '{skill_name}' not found or timed out")

async def handle_event_subscribe(ws, msg: dict):
    event_type = msg.get("event_type", "*")
    if event_type not in state.subscribers:
        state.subscribers[event_type] = set()
    state.subscribers[event_type].add(ws)
    return ok_msg("subscribed", event_type=event_type)

async def handle_event_publish(ws, msg: dict):
    event_type = msg.get("event_type", "fabric.event")
    payload = msg.get("payload", {})
    await broadcast_event(event_type, payload)
    return ok_msg("published", event_type=event_type)

async def handle_query(ws, msg: dict):
    """Query the memory log."""
    limit = min(msg.get("limit", 50), 200)
    event_type = msg.get("event_type")
    results = []
    for e in reversed(state.events):
        if event_type and e.get("event_type") != event_type:
            continue
        results.append(e)
        if len(results) >= limit:
            break
    return ok_msg("query_results", events=results, count=len(results))

# ── Event helpers ──

async def broadcast_event(event_type: str, payload: dict):
    event = {"event_type": event_type, "timestamp": _ts(), "payload": payload}
    state.events.append(event)
    # Trim event log
    if len(state.events) > 10000:
        state.events = state.events[-5000:]
    # Notify subscribers
    targets = state.subscribers.get(event_type, set()) | state.subscribers.get("*", set())
    msg = json.dumps(ok_msg("event", event_type=event_type, payload=payload))
    dead = set()
    for sub in targets:
        try:
            await sub.send(msg)
        except Exception:
            dead.add(sub)
    for d in dead:
        for v in state.subscribers.values():
            v.discard(d)

async def log_event(event_type: str, payload: dict):
    state.events.append({"event_type": event_type, "timestamp": _ts(), "payload": payload})

# ── Main handler ──

async def fabric_handler(ws: websockets.WebSocketServerProtocol, path: str):
    agent_id = None
    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await ws.send(json.dumps(err_msg("Invalid JSON")))
                continue

            mtype = msg.get("type", "")
            if mtype == "register":
                resp = await handle_register(ws, msg)
                agent_id = resp.get("agent_id")
            elif mtype == "heartbeat":
                resp = await handle_heartbeat(ws, msg)
            elif mtype == "discover":
                resp = await handle_discover(ws, msg)
            elif mtype == "assign":
                resp = await handle_assign(ws, msg)
            elif mtype == "skill:publish":
                resp = await handle_skill_publish(ws, msg)
            elif mtype == "skill:invoke":
                resp = await handle_skill_invoke(ws, msg)
            elif mtype == "event:subscribe":
                resp = await handle_event_subscribe(ws, msg)
            elif mtype == "event:publish":
                resp = await handle_event_publish(ws, msg)
            elif mtype == "query":
                resp = await handle_query(ws, msg)
            elif mtype == "ping":
                resp = ok_msg("pong")
            else:
                resp = err_msg(f"Unknown message type: {mtype}")

            # Mirror request_id back for correlation
            if msg.get("request_id"):
                resp["request_id"] = msg["request_id"]

            await ws.send(json.dumps(resp))

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if agent_id and agent_id in state.agents:
            state.agents[agent_id].status = "offline"
            state.agents[agent_id].websocket = None
            await broadcast_event("agent.disconnected", {"agent_id": agent_id})

# ── Dashboard API ──

async def dashboard_handler(reader, writer):
    html_path = Path(__file__).parent / "dashboard" / "index.html"
    if html_path.exists():
        content = html_path.read_text(encoding="utf-8")
    else:
        content = "<h1>Agent Fabric</h1><p>Dashboard not found.</p>"
    resp = f"HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {len(content.encode('utf-8'))}\r\nConnection: close\r\n\r\n{content}"
    writer.write(resp.encode()); await writer.drain()
    writer.close(); await writer.wait_closed()

async def stats_handler(reader, writer):
    data = {
        "agents": {aid: {"type": a.agent_type, "status": a.status,
                         "caps": a.capabilities, "skills": len(a.skills),
                         "uptime": _now() - state.start_time}
                   for aid, a in state.agents.items()},
        "agent_count": len(state.agents),
        "event_count": len(state.events),
        "task_count": len(state.active_tasks),
        "uptime": _now() - state.start_time,
    }
    body = json.dumps(data)
    resp = f"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n{body}"
    writer.write(resp.encode()); await writer.drain()
    writer.close(); await writer.wait_closed()

# ── Periodic cleanup ──

async def heartbeat_monitor():
    """Auto-deregister agents that stop heartbeating."""
    while True:
        await asyncio.sleep(30)
        now = _now()
        dead = []
        for aid, agent in state.agents.items():
            if now - agent.last_heartbeat > 90 and agent.status == "online":
                agent.status = "stale"
                agent.websocket = None
                dead.append(aid)
        for aid in dead:
            await broadcast_event("agent.stale", {"agent_id": aid})

# ── Main ──

async def main():
    print(f"  Agent Fabric Gateway")
    print(f"  ws://127.0.0.1:{GATEWAY_PORT}/ws")
    print(f"  Dashboard: http://127.0.0.1:{HTTP_PORT}")

    ws_server = await websockets.serve(
        fabric_handler, "127.0.0.1", GATEWAY_PORT,
        ping_interval=None, ping_timeout=None,
    )
    http_server = await asyncio.start_server(dashboard_handler, "127.0.0.1", HTTP_PORT)
    stats_server = await asyncio.start_server(stats_handler, "127.0.0.1", HTTP_PORT + 10)

    await asyncio.gather(
        ws_server.wait_closed(),
        http_server.serve_forever(),
        stats_server.serve_forever(),
        heartbeat_monitor(),
    )

if __name__ == "__main__":
    asyncio.run(main())
