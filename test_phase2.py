# test_phase2.py
import asyncio
from roles.coordinator import CoordinatorNode
from roles.executor import ExecutorNode

async def main():
    coordinator = CoordinatorNode("VP_CO")
    executor = ExecutorNode("EP_1")

    t1 = asyncio.create_task(coordinator.start())
    t2 = asyncio.create_task(executor.start())

    print("--- Phase 2: Logic & Execution ---")
    await asyncio.sleep(2) 

    task_code = """
    import math
    print('    -> Inside Executor: Calculating square root...')
    val = args['num1'] + args['num2']
    output = math.sqrt(val)
    """
    
    task_args = {"num1": 100, "num2": 44} 
    await coordinator.submit_task("EP_1", task_code, task_args)

    await asyncio.sleep(3)

    print("\n--- Sending a Bad Task (Error Handling Test) ---")
    bad_code = "output = 10 / 0"
    await coordinator.submit_task("EP_1", bad_code, {})
    
    await asyncio.sleep(2)

    t1.cancel()
    t2.cancel()
    print("Test Finished.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
