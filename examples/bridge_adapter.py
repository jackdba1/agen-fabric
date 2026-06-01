#!/usr/bin/env python3
"""UFO² Bridge Fabric Adapter — connects the UFO² bridge to the Agent Fabric.

Run this alongside or instead of the bridge server to give the bridge
fabric membership. Accepts tasks from any fabric agent and executes them
via UFO².

Usage:
  python examples\bridge_adapter.py
"""

import asyncio, json, os, sys, re
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from client import FabricAgent

UFO_VENV = os.environ.get("UFO_VENV", r"C:\UFO\.venv\Scripts\python.exe")
UFO_ROOT = os.environ.get("UFO_ROOT", r"C:\UFO")

async def ufo_exec(request: str) -> dict:
    """Execute a UFO² task."""
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
    return {
        "status": "success" if success else "failure",
        "output": "\n".join(lines[-30:]),
        "line_count": len(lines),
    }


async def main():
    agent = FabricAgent(
        agent_id="ufo2_bridge",
        agent_type="device",
        capabilities=[
            "windows_apps", "file_mgmt", "web_browse",
            "shell", "screenshots", "uia_control",
        ],
        skills={
            "ufo_exec": {"description": "Execute a UFO2 desktop automation task"},
            "get_status": {"description": "Check UFO2 agent status"},
        },
        metadata={
            "hostname": os.environ.get("COMPUTERNAME", "l5"),
            "ufo_root": UFO_ROOT,
        },
    )

    print(f"\n[bridge] Connecting to fabric...")
    ok = await agent.connect()
    if not ok:
        print("[bridge]  FAILED — is the gateway running?")
        return

    # Task handler — execute UFO2 tasks
    async def handle_task(msg: dict):
        task = msg.get("task", {})
        request = task.get("request", task.get("command", ""))
        task_id = msg.get("task_id", "unknown")
        print(f"[bridge] TASK: {task_id} — {request[:80]}")
        result = await ufo_exec(request)
        if agent._ws:
            resp = {"type": "task_result", "task_id": task_id, "agent_id": "ufo2_bridge", "result": result}
            await agent._ws.send(json.dumps(resp))
            print(f"[bridge]  → result: {result['status']} ({result['line_count']} lines)")

    # Skill handler
    async def handle_skill(msg: dict):
        skill = msg.get("skill_name", "")
        if skill == "ufo_exec":
            result = await ufo_exec(msg.get("parameters", {}).get("request", ""))
        elif skill == "get_status":
            result = {"status": "ok", "agent": "ufo2_bridge", "ufo_root": UFO_ROOT}
        else:
            result = {"status": "error", "reason": f"Unknown skill: {skill}"}
        if agent._ws:
            resp = {"type": "skill_result", "task_id": msg.get("task_id"), "agent_id": "ufo2_bridge", "result": result}
            await agent._ws.send(json.dumps(resp))

    agent.on("task", handle_task)
    agent.on("skill_invoke", handle_skill)
    await agent.start_heartbeat(25)
    await agent.subscribe("*")

    # Discover
    peers = await agent.discover()
    print(f"[bridge]  Fabric agents: {len(peers)}")
    for p in peers:
        print(f"    {p['agent_id']} [{p['agent_type']}]")

    print("[bridge]  Ready.\n")

    try:
        while True:
            await asyncio.sleep(10)
    except KeyboardInterrupt:
        print("\n[bridge] Shutting down...")
        await agent.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
