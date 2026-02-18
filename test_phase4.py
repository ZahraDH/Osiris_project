# test_phase4.py
import asyncio
from core.node import Node
from roles.coordinator import CoordinatorNode
from roles.executor import ExecutorNode
from roles.malicious import MaliciousExecutor

async def main():
    print("--- Phase 4: Liveness & Reassignment Test ---")
    coordinator = CoordinatorNode("VP_CO")
    good_node = ExecutorNode("EP_Good")
    bad_node = MaliciousExecutor("EP_Bad") 
    nodes = [coordinator, good_node, bad_node]
    server_tasks = [asyncio.create_task(n.start()) for n in nodes]
    await asyncio.sleep(2)
    target_nodes = ["EP_Bad", "EP_Good"]
    
    code = "output = args['a'] * args['b']"
    args = {"a": 10, "b": 5}
    expected = 50

    print("\n--- Sending Task to [EP_Bad, EP_Good] ---")
    await coordinator.submit_task_with_retry(target_nodes, code, args, expected_check=expected)

    print("\n--- Test Finished. Shutting down... ---")
    for task in server_tasks:
        task.cancel()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
