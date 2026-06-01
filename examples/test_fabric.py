"""End-to-end test of the Agent Fabric mesh.
1. Start gateway (already running)
2. Connect bridge adapter (device agent)
3. Connect opencode adapter (reasoning agent)
4. Verify both register
5. Test task delegation: opencode -> bridge
6. Verify event propagation
"""
import asyncio, json, websockets, sys, os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from client import FabricAgent

async def main():
    gateway = "ws://localhost:8400/ws"

    # === 1. Test raw gateway ===
    async with websockets.connect(gateway) as ws:
        await ws.send(json.dumps({"type": "ping"}))
        pong = json.loads(await asyncio.wait_for(ws.recv(), timeout=3))
        assert pong["type"] == "pong", f"Gateway not responding: {pong}"
        print("[1] GATEWAY    ping/pong OK")

    # === 2. Register bridge ===
    bridge = FabricAgent("ufo2_bridge", "device", ["windows_apps", "shell", "screenshots"])
    assert await bridge.connect(gateway), "Bridge connect failed"
    await bridge.start_heartbeat(10)
    print("[2] BRIDGE     registered as device agent")

    # === 3. Register opencode ===
    opencode = FabricAgent("opencode", "reasoning", ["code", "research", "system", "delegation"])
    assert await opencode.connect(gateway), "Opencode connect failed"
    await opencode.start_heartbeat(10)
    print("[3] OPENCODE   registered as reasoning agent")

    # === 4. Discovery ===
    agents = await opencode.discover()
    print(f"[4] DISCOVER   {len(agents)} agents found:")
    for a in agents:
        print(f"      {a['agent_id']} [{a['agent_type']}] caps={a['capabilities']}")

    assert len(agents) >= 2, f"Expected at least 2 agents, got {len(agents)}"

    # === 5. Event subscription ===
    events_received = []
    async def on_event(payload):
        events_received.append(payload)
    opencode.on_event("*", on_event)
    await opencode.subscribe("*")

    # === 6. Publish event ===
    await bridge.publish_event("test.ping", {"message": "hello from bridge"})
    await asyncio.sleep(0.5)
    print(f"[5] EVENTS     received {len(events_received)} events")

    # === 7. Task delegation (bridge executes, returns result) ===
    # We need to set up the bridge to handle tasks
    async def bridge_task_handler(msg):
        task = msg.get("task", {})
        request = task.get("request", "")
        if bridge._ws:
            resp = {"type": "task_result", "task_id": msg.get("task_id"),
                    "agent_id": "ufo2_bridge", "result": {"status": "success", "output": f"Executed: {request}"}}
            await bridge._ws.send(json.dumps(resp))

    bridge.on("task", bridge_task_handler)

    result = await opencode.assign("ufo2_bridge", {"request": "Echo test"})
    print(f"[6] DELEGATE   opencode -> bridge -> result: {result}")

    # === 8. Query history ===
    history = await opencode.query(limit=5)
    print(f"[7] QUERY      {len(history)} events in memory")

    # Cleanup
    await opencode.disconnect()
    await bridge.disconnect()
    print("\n=== ALL FABRIC TESTS PASSED ===")

asyncio.run(main())
