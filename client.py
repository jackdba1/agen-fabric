"""Agent Fabric Client — embed in any agent to join the fabric.

Usage:
    from client import FabricAgent
    agent = FabricAgent("opencode", "reasoning", ["code", "analysis", "research"])
    await agent.connect("ws://localhost:8400/ws")
    await agent.start_heartbeat(interval=25)

    # Discover other agents
    devices = await agent.discover(agent_type="device")

    # Delegate a task
    result = await agent.assign("ufo2_bridge", {"request": "Open Notepad"})

    # Publish a skill
    await agent.publish_skill("summarize", {"description": "Summarize text", ...})

    # Subscribe to events
    await agent.subscribe("task.completed")
"""

import json, uuid, asyncio, os, time
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine, List, Optional
import websockets

FABRIC_URL = os.environ.get("FABRIC_URL", "ws://localhost:8400/ws")

def _id() -> str: return uuid.uuid4().hex[:8]

class FabricAgent:
    def __init__(
        self,
        agent_id: str,
        agent_type: str = "device",
        capabilities: Optional[List[str]] = None,
        skills: Optional[dict] = None,
        metadata: Optional[dict] = None,
    ):
        self.agent_id = agent_id
        self.agent_type = agent_type
        self.capabilities = capabilities or []
        self.skills = skills or {}
        self.metadata = metadata or {}
        self._ws = None
        self._heartbeat_task = None
        self._listen_task = None
        self._handlers: dict[str, Callable] = {}
        self._pending_tasks: dict[str, asyncio.Future] = {}
        self._pending: dict[str, asyncio.Future] = {}  # request_id -> future for _send responses
        self._event_callbacks: dict[str, list[Callable]] = {}
        self._connected = False

    async def connect(self, url: Optional[str] = None, timeout: int = 10) -> bool:
        """Connect to the fabric gateway."""
        target = url or FABRIC_URL
        try:
            self._ws = await asyncio.wait_for(
                websockets.connect(target, ping_interval=None, ping_timeout=None),
                timeout=timeout
            )
        except Exception:
            self._ws = None
            return False

        await self._register()
        self._listen_task = asyncio.create_task(self._listen())
        self._connected = True
        return True

    async def disconnect(self):
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._listen_task:
            self._listen_task.cancel()
        if self._ws:
            try: await self._ws.close()
            except: pass
        self._connected = False

    # ── Fabric operations ──

    async def discover(self, agent_type: Optional[str] = None, capability: Optional[str] = None) -> List[dict]:
        msg = {"type": "discover", "agent_id": self.agent_id}
        if agent_type: msg["agent_type"] = agent_type
        if capability: msg["capability"] = capability
        resp = await self._send(msg)
        return resp.get("agents", [])

    async def assign(self, target_id: str, task: dict, timeout: int = 120) -> Optional[dict]:
        """Delegate a task to another agent and wait for the result."""
        msg = {
            "type": "assign", "agent_id": self.agent_id,
            "target_id": target_id, "task": task,
        }
        resp = await self._send(msg)
        if resp.get("type") == "assign" and resp.get("status") == "completed":
            return resp.get("result")
        return None

    async def publish_skill(self, name: str, skill: dict) -> bool:
        resp = await self._send({
            "type": "skill:publish", "agent_id": self.agent_id,
            "skill_name": name, "skill": skill,
        })
        return resp.get("type") == "skill_published"

    async def invoke_skill(self, name: str, parameters: Optional[dict] = None) -> Optional[dict]:
        resp = await self._send({
            "type": "skill:invoke", "agent_id": self.agent_id,
            "skill_name": name, "parameters": parameters or {},
        })
        if resp.get("type") == "skill_invoked":
            return resp.get("result")
        return None

    async def subscribe(self, event_type: str = "*"):
        await self._send({"type": "event:subscribe", "agent_id": self.agent_id, "event_type": event_type})

    async def publish_event(self, event_type: str, payload: dict):
        await self._send({"type": "event:publish", "agent_id": self.agent_id, "event_type": event_type, "payload": payload})

    async def query(self, event_type: Optional[str] = None, limit: int = 50) -> List[dict]:
        resp = await self._send({"type": "query", "agent_id": self.agent_id, "event_type": event_type, "limit": limit})
        return resp.get("events", [])

    async def ping(self) -> bool:
        try:
            resp = await self._send({"type": "ping", "agent_id": self.agent_id})
            return resp.get("type") == "pong"
        except Exception:
            return False

    # ── Callbacks ──

    def on(self, msg_type: str, handler: Callable):
        """Register a handler for incoming messages (task, skill_invoke, event)."""
        self._handlers[msg_type] = handler

    def on_event(self, event_type: str, callback: Callable[[dict], Coroutine]):
        """Register a callback for fabric events."""
        if event_type not in self._event_callbacks:
            self._event_callbacks[event_type] = []
        self._event_callbacks[event_type].append(callback)

    # ── Heartbeat ──

    async def start_heartbeat(self, interval: int = 25):
        async def _loop():
            while self._connected:
                try:
                    await self._send({"type": "heartbeat", "agent_id": self.agent_id})
                except Exception:
                    pass
                await asyncio.sleep(interval)
        self._heartbeat_task = asyncio.create_task(_loop())

    # ── Internals ──

    async def _register(self):
        resp = await self._send({
            "type": "register", "agent_id": self.agent_id,
            "agent_type": self.agent_type,
            "capabilities": self.capabilities,
            "skills": self.skills,
            "metadata": self.metadata,
        })
        return resp

    async def _send(self, msg: dict) -> dict:
        """Send a message and wait for a response (routed from _listen)."""
        if not self._ws:
            raise ConnectionError("Not connected to fabric")
        request_id = msg.get("request_id", _id())
        msg["request_id"] = request_id
        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending[request_id] = fut
        await self._ws.send(json.dumps(msg))
        try:
            return await asyncio.wait_for(fut, timeout=30)
        except asyncio.TimeoutError:
            self._pending.pop(request_id, None)
            raise

    async def _listen(self):
        """Background listener — routes all incoming messages."""
        try:
            async for raw in self._ws:
                if not isinstance(raw, str):
                    continue
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                # Check if this is a response to a pending _send request
                request_id = msg.get("request_id")
                if request_id and request_id in self._pending:
                    fut = self._pending.pop(request_id)
                    if not fut.done():
                        fut.set_result(msg)
                    continue

                mtype = msg.get("type", "")

                # Events
                if mtype == "event":
                    et = msg.get("event_type", "")
                    payload = msg.get("payload", {})
                    for cb in self._event_callbacks.get(et, []):
                        try: await cb(payload)
                        except: pass
                    for cb in self._event_callbacks.get("*", []):
                        try: await cb(payload)
                        except: pass

                # Registered handlers
                elif mtype in self._handlers:
                    try: await self._handlers[mtype](msg)
                    except: pass

        except websockets.exceptions.ConnectionClosed:
            self._connected = False
        except Exception:
            self._connected = False
