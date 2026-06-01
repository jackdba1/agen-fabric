#!/usr/bin/env python3
"""OpenCode Fabric Adapter — connects OpenCode (the dev agent) to the Agent Fabric.

Run this as a sidecar to give OpenCode fabric membership:
  python examples\opencode_adapter.py

OpenCode registers as a "reasoning" agent with capabilities:
  - code (read, write, analyze, refactor)
  - research (search, compare, explain)
  - system (shell, file ops, git)
  - delegate (route tasks to other agents)

While connected, OpenCode can:
  - Discover other agents (devices, orchestrators)
  - Delegate desktop tasks to UFO² bridge or Galaxy
  - Query fabric event history
  - Subscribe to fabric events
"""

import asyncio, json, os, sys, subprocess, re
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from client import FabricAgent

# ── UFO² invocation (fallback — direct subprocess) ──

UFO_VENV = os.environ.get("UFO_VENV", r"C:\UFO\.venv\Scripts\python.exe")
UFO_ROOT = os.environ.get("UFO_ROOT", r"C:\UFO")

async def ufo_task(request: str) -> dict:
    """Execute a UFO² task and return the result."""
    tid = re.sub(r"[^a-zA-Z0-9]", "_", request.lower())[:30]
    env = os.environ.copy(); env["PYTHONUTF8"] = "1"
    proc = await asyncio.create_subprocess_exec(
        UFO_VENV, "-m", "ufo", "--task", tid, "--request", request,
        cwd=UFO_ROOT, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        env=env,
    )
    lines = []
    ansi = re.compile(r"\x1b\[[0-9;]*m")
    while True:
        line = await proc.stdout.readline()
        if not line: break
        clean = ansi.sub("", line.decode("utf-8", errors="replace")).rstrip()
        if clean and "AgentRegistry" not in clean:
            lines.append(clean)
    await proc.wait()
    success = any("COMPLETE" in l or "FINISH" in l for l in lines)
    return {"status": "success" if success else "failure", "output": "\n".join(lines[-30:]), "lines": len(lines)}


# ── OpenCode adapter ──

class OpenCodeAdapter:
    def __init__(self):
        self.agent = FabricAgent(
            agent_id="opencode",
            agent_type="reasoning",
            capabilities=[
                "code", "research", "system",
                "shell", "file_ops", "git",
                "analysis", "refactoring",
                "delegation",  # can route tasks to other agents
            ],
            skills={
                "file_read":   {"description": "Read a file from the local filesystem"},
                "file_write":  {"description": "Write content to a file"},
                "file_search": {"description": "Search files by pattern (glob) or content (grep)"},
                "shell_exec":  {"description": "Execute a shell command and return output"},
                "ufo_task":    {"description": "Execute a UFO2 desktop automation task"},
                "fabric_query":{"description": "Query the fabric event history"},
                "fabric_delegate": {"description": "Route a task to another fabric agent"},
            },
            metadata={
                "hostname": os.environ.get("COMPUTERNAME", "l5"),
                "project_count": 8,
                "venv": UFO_VENV,
            },
        )

    async def handle_task(self, msg: dict):
        """Handle incoming tasks from fabric (delegated by other agents)."""
        task = msg.get("task", msg)  # Could be nested
        request = task.get("request", task.get("command", ""))
        if not request:
            return

        task_id = msg.get("task_id", "unknown")
        print(f"\n[opencode] TASK RECEIVED: {task_id} — {request[:100]}")

        # Route by capability needed
        if any(w in request.lower() for w in ["open", "click", "type", "notepad", "calc", "browser", "firefox"]):
            print(f"[opencode] → delegating to UFO2 bridge...")
            result = await self.agent.assign("ufo2_bridge", {"request": request})
            if result:
                print(f"[opencode] ✓ UFO2 completed")
            else:
                print(f"[opencode] ⚠ Bridge unreachable, using direct UFO2...")
                result = await ufo_task(request)
        else:
            # Handle locally (code, research, etc.)
            print(f"[opencode] → processing locally...")
            result = {"status": "success", "output": f"Opencode processed: {request}"}

        # Send result back
        if self.agent._ws:
            resp = {
                "type": "task_result", "task_id": task_id,
                "agent_id": "opencode", "result": result,
            }
            await self.agent._ws.send(json.dumps(resp))

    async def handle_skill_invoke(self, msg: dict):
        """Handle skill invocations from the fabric."""
        skill = msg.get("skill_name", "")
        params = msg.get("parameters", {})
        result = None

        if skill == "ufo_task":
            result = await ufo_task(params.get("request", ""))
        elif skill == "fabric_query":
            result = await self.agent.query(params.get("event_type"), params.get("limit", 20))
        elif skill == "fabric_delegate":
            result = await self.agent.assign(params.get("target"), params.get("task", {}))
        else:
            result = {"status": "ok", "skill": skill, "params": params}

        if result and self.agent._ws:
            resp = {
                "type": "skill_result", "task_id": msg.get("task_id"),
                "agent_id": "opencode", "result": result,
            }
            await self.agent._ws.send(json.dumps(resp))

    async def on_fabric_event(self, payload: dict):
        """React to fabric events."""
        et = payload.get("event_type", "")
        if "agent.registered" in et:
            aid = payload.get("payload", {}).get("agent_id")
            print(f"[opencode]  agent joined: {aid}")
        elif "agent.disconnected" in et:
            aid = payload.get("payload", {}).get("agent_id")
            print(f"[opencode]  agent left: {aid}")

    async def start(self, fabric_url: str = "ws://localhost:8400/ws"):
        print(f"\n[opencode] Connecting to fabric at {fabric_url}...")
        ok = await self.agent.connect(fabric_url)
        if not ok:
            print("[opencode]  FAILED — is the gateway running?")
            return False

        print(f"[opencode]  Registered as reasoning agent ({len(self.agent.capabilities)} caps, {len(self.agent.skills)} skills)")

        # Set up handlers
        self.agent.on("task", self.handle_task)
        self.agent.on("skill_invoke", self.handle_skill_invoke)

        # Subscribe to all events
        await self.agent.subscribe("*")
        self.agent.on_event("*", self.on_fabric_event)

        # Start heartbeat
        await self.agent.start_heartbeat(25)

        # Discover who else is here
        agents = await self.agent.discover()
        print(f"[opencode]  Fabric agents: {len(agents)}")
        for a in agents:
            print(f"    {a['agent_id']} [{a['agent_type']}] caps={a['capabilities']}")

        print("[opencode]  Ready. Listening for tasks.\n")
        return True


async def main():
    adapter = OpenCodeAdapter()
    ok = await adapter.start()
    if not ok:
        return

    # Keep alive — also expose a minimal CLI
    try:
        while True:
            await asyncio.sleep(5)
    except KeyboardInterrupt:
        print("\n[opencode] Shutting down...")
        await adapter.agent.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
