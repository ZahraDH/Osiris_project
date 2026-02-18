# test_phase3.py (Corrected)
import asyncio
from roles.coordinator import CoordinatorNode
from roles.executor import ExecutorNode
from roles.malicious import MaliciousExecutor

async def main():
    coordinator = CoordinatorNode("VP_CO")
    good_node = ExecutorNode("EP_Good")
    bad_node = MaliciousExecutor("EP_Bad")

    print("--- Starting Nodes ---")
    
    t1 = asyncio.create_task(coordinator.start())
    t2 = asyncio.create_task(good_node.start())
    t3 = asyncio.create_task(bad_node.start())
    
    await asyncio.sleep(2)
    
    print("\n--- Phase 3: Byzantine Fault Tolerance Test ---")

    code = "output = args['a'] + args['b']"
    args = {"a": 10, "b": 20}
    EXPECTED = 30 
    print("\n[1] Testing Good Node (EP_Good)...")
    await coordinator.submit_task("EP_Good", code, args, expected_check=EXPECTED) 
    await asyncio.sleep(2)


    print("\n[2] Testing Malicious Node (EP_Bad)...")
    await coordinator.submit_task("EP_Bad", code, args, expected_check=EXPECTED)
    await asyncio.sleep(2)

    print("\n--- Test Finished. Shutting down... ---")
    t1.cancel()
    t2.cancel()
    t3.cancel()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
