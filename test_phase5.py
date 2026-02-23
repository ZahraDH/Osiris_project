import asyncio
import json
import os
from roles.coordinator import CoordinatorNode
from roles.executor import ExecutorNode
from roles.client import Client
from roles.malicious import MaliciousExecutor
from roles.verifier import VerifierNode
from core.crypto import CryptoManager

CONFIG_FILE = "config.json"

async def run_test():
    print("--- STARTING OSIRIS-BFT TRANSACTION SYSTEM TEST ---")
    cryptos = {}
    for node_id in ["CO_1", "CO_2", "CO_3", "EP_1", "EP_Bad", "VP_1", "VP_2"]:
        c = CryptoManager(node_id)
        c.load_keys()
        cryptos[node_id] = c
        
    co1 = CoordinatorNode("CO_1", cryptos["CO_1"], CONFIG_FILE)
    co2 = CoordinatorNode("CO_2", cryptos["CO_2"], CONFIG_FILE)
    co3 = CoordinatorNode("CO_3", cryptos["CO_3"], CONFIG_FILE)
    
    ep1 = ExecutorNode("EP_1", cryptos["EP_1"], CONFIG_FILE)          
    ep_bad = MaliciousExecutor("EP_Bad", cryptos["EP_Bad"], CONFIG_FILE)
    
    vp1 = VerifierNode("VP_1", cryptos["VP_1"], CONFIG_FILE)
    vp2 = VerifierNode("VP_2", cryptos["VP_2"], CONFIG_FILE)

    client = Client("Client", CONFIG_FILE)
    
    tasks = []
    nodes = [co1, co2, co3, ep1, ep_bad, vp1, vp2, client]
    for node in nodes:
        tasks.append(asyncio.create_task(node.start()))

    print("--- Waiting for network to stabilize (2s) ---")
    await asyncio.sleep(2)

    print("\n--- SCENARIO A: STATE REPLICATION ---")
    
    tx1 = {"op": "SET", "args": {"key": "price", "value": 100}}
    await client.submit_transaction(tx1, targets=["CO_1"])

    tx2 = {"op": "SET", "args": {"key": "tax", "value": 20}}
    await client.submit_transaction(tx2, targets=["CO_1"])

    await asyncio.sleep(2) 

    print(f"   [Test Check] EP_1 State: {ep1.store._state}")
    print(f"   [Test Check] EP_Bad State: {ep_bad.store._state}")

    if ep1.store.get('price') == 100 and ep1.store.get('tax') == 20:
        print("PASS: Honest Executor has correct state.")
    else:
        print("FAIL: Honest Executor state mismatch.")


    print("\n--- SCENARIO B: COMPUTATION & FRAUD DETECTION (OsirisBFT) ---")
    print("   [Client] Sending Multiple SUM_ALL requests to ensure EP_Bad gets selected...")
    

    for i in range(1, 4):
        print(f"\n   -> Submitting Request {i} ...")
        tx_sum = {"op": "SUM_ALL", "args": {"req_id": i}}
        await client.submit_transaction(tx_sum, targets=["CO_1", "CO_2", "CO_3"])
        await asyncio.sleep(1.5) 
        
    print("\n--- Waiting for Verifications and Reassignments (5s) ---")
    await asyncio.sleep(5) 
    
    print("--- TEST COMPLETE ---")
    

if __name__ == "__main__":
    if not os.path.exists(CONFIG_FILE):
        print(f"ERROR: '{CONFIG_FILE}' not found! Please create it first.")
        exit(1)
    try:
        asyncio.run(run_test())
    except KeyboardInterrupt:
        pass
