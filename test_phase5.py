# test_phase1.py
import asyncio
import json
import os
from roles.coordinator import CoordinatorNode
from roles.executor import ExecutorNode
from roles.client import Client
from roles.malicious import MaliciousExecutor
from roles.executor import ExecutorNode
from core.types import MessageType
from core.crypto import CryptoManager

CONFIG_FILE = "config.json"



async def run_test():
    print("--- STARTING TRANSACTION SYSTEM TEST ---")

    crypto_exe1 = CryptoManager("EP_1")     
    crypto_bad = CryptoManager("EP_Bad")
    crypto_co = CryptoManager("VP_CO")

    crypto_exe1.load_keys()
    crypto_bad.load_keys()
    crypto_co.load_keys()

    
    good_node = ExecutorNode("EP_1", crypto_exe1, CONFIG_FILE)          
    bad_node = MaliciousExecutor("EP_Bad", crypto_bad, CONFIG_FILE)
    coordinator = CoordinatorNode("VP_CO", crypto_co, CONFIG_FILE)

    client = Client("Client", CONFIG_FILE)


    tasks = []
    nodes = [coordinator, good_node, bad_node, client]
    for node in nodes:
        tasks.append(asyncio.create_task(node.start()))

    print("--- Waiting for network to stabilize (2s) ---")
    await asyncio.sleep(2)

    print("\n--- SCENARIO A: STATE REPLICATION ---")

    tx1 = {"op": "SET", "args": {"key": "price", "value": 100}}
    await client.submit_transaction(tx1)

    tx2 = {"op": "SET", "args": {"key": "tax", "value": 20}}
    await client.submit_transaction(tx2)

    await asyncio.sleep(1)

    print(f"   [Test Check] EP1 State: {good_node.store._state}")
    print(f"   [Test Check] EP_Bad State: {bad_node.store._state}")

    if good_node.store.get('price') == 100 and good_node.store.get('tax') == 20:
        print("PASS: Honest Executor has correct state.")
    else:
        print("FAIL: Honest Executor state mismatch.")


    print("\n--- SCENARIO B: COMPUTATION & FAULT TOLERANCE ---")


    tx_sum = {"op": "SUM_ALL", "args": {}}

    print("   [Client] Requesting SUM_ALL (Expected: 120)... sending requests...")

    for i in range(1, 4):
        print(f"\n   --- Request #{i} ---")
        await client.submit_transaction(tx_sum)
        await asyncio.sleep(1.5)

    print("\n--- TEST COMPLETED ---")

    for node in nodes:
        await node.stop()
    for t in tasks:
        t.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(run_test())
    except KeyboardInterrupt:
        pass