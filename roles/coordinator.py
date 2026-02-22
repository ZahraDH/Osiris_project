# roles/coordinator.py
import asyncio
import json
import uuid
import time
from core.node import Node
from utils.transfer_to_string import get_deterministic_string
from core.state import VersionedKVStore
from core.transaction import TransactionEngine
from core.types import MessageType

class CoordinatorNode(Node):
    def __init__(self, node_id,crypto_manager, config_path=None):
        if config_path:
            super().__init__(node_id, config_path)
        else:
            super().__init__(node_id)
        self.pending_tasks = {}
        self.crypto = crypto_manager
        self.oracle_state = VersionedKVStore()
        self.seq_counter = 0
        self.tx_engine = TransactionEngine()

    async def process_message(self, message):
        if 'signature' in message:
            sender_id = message.get("sender_id")
            signature = message.get("signature")
            content_str = message.get("content") 
            

            is_valid = self.crypto.verify(sender_id, content_str, signature)
            
            if not is_valid:
                print(f"[VP_CO] Invalid signature on result from {sender_id}. Dropping.")
                return {"status": "error"}
            else:
                print(f"[VP_CO] Valid signature from {sender_id}")
            
            try:
                msg_data = json.loads(content_str)
            except json.JSONDecodeError:
                return {"status": "error"}
        else:
            msg_data = message

        msg_type = msg_data.get("type")
        
        if msg_type == "CLIENT_REQUEST" or msg_type == getattr(MessageType, 'CLIENT_REQUEST', 'CLIENT_REQUEST'):
            payload = msg_data.get("payload", msg_data)
            await self.handle_client_request(payload)
            return {"status": "received"}
        
        if msg_type == "RESULT":
            task_id = msg_data.get("task_id")
            result = msg_data.get("result")
            sender = msg_data.get("sender")
            
            if task_id in self.pending_tasks:
                future = self.pending_tasks[task_id]
                if not future.done():
                    future.set_result((sender, result))
            
            return {"status": "received"}
            
        return await super().process_message(message)
    
    
    async def handle_client_request(self, tx):
        if isinstance(tx, str):
            try:
                tx = json.loads(tx)
            except:
                pass

        op = tx.get("op", "").upper()
        if op in ["SET", "ADD", "SUB"]:
            print(f"[VP_CO] Client requests WRITE: {op}")
            await self.propose_state_update(tx)
        else:
            print(f"[VP_CO] Client requests READ: {op}")
            await self.assign_compute_task(tx)

    async def propose_state_update(self, tx):
        self.seq_counter += 1
        seq = self.seq_counter

        try:
            self.tx_engine.execute(tx, mode='write', state=self.oracle_state)
            self.oracle_state.commit_version(seq)
            print(f"[VP_CO] Proposed Update Seq {seq}: {tx.get('args')} -> Committed to Oracle")
        except Exception as e:
            print(f"[VP_CO] Oracle Error: {e}")
            return

        update_msg = {
            "type": "STATE_UPDATE", 
            "seq": seq, 
            "tx": tx
        }
        
        executors = [nid for nid in self.peers if nid.startswith("EP")]
        for ep in executors:
            await self._send_signed_message_fire_and_forget(ep, update_msg)

    async def assign_compute_task(self, tx):
        task_id = str(uuid.uuid4())[:8]
        current_version = self.seq_counter
        snapshot = self.oracle_state.get_version(current_version)

        try:
            expected_result = self.tx_engine.execute(tx, mode='read', state=snapshot)
        except Exception as e:
            print(f"[VP_CO] Oracle Compute Error: {e}")
            return

        payload = {
            "type": "COMPUTE_TASK", 
            "task_id": task_id, 
            "tx": tx, 
            "version": current_version,
            "expected_check": expected_result 
        }

        executors = [nid for nid in self.peers if nid.startswith("EP")]
        
        if "EP_Bad" in executors:
            executors.remove("EP_Bad")
            executors.insert(0, "EP_Bad")

        for executor_id in executors:
            print(f"\n[VP_CO] Assigning Task {task_id} to {executor_id}. Expected: {expected_result}")
            
            success = await self._send_task_and_wait(executor_id, task_id, payload, expected_result)
            
            if success:
                print(f"[VP_CO] Task {task_id} completed successfully by {executor_id}.")
                return
            else:
                print(f"[VP_CO] Task failed or mismatched on {executor_id}. Re-assigning...")
                
        print(f"[VP_CO] CRITICAL: Task {task_id} failed on ALL available nodes.")
        

    async def submit_task_with_retry(self, target_nodes, args, expected_check=None):
        task_id = str(uuid.uuid4())[:8]
        
        for node_id in target_nodes:
            print(f"\n[VP_CO] Attempting to assign Task {task_id} to {node_id}...")
            
            success = await self._send_task_and_wait(node_id, task_id, args, expected_check)
            
            if success:
                print(f"[VP_CO] Task {task_id} completed successfully by {node_id}.")
                return
            else:
                print(f"[VP_CO] Task {task_id} failed on {node_id}. Re-assigning...")
        
        print(f"[VP_CO] CRITICAL: Task {task_id} failed on ALL available nodes.")
        

    async def _send_task_and_wait(self, target_node_id, task_id, payload_dict, expected_check):
        if target_node_id not in self.peers:
            return False

        target_info = self.peers[target_node_id]
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.pending_tasks[task_id] = future
        
        content_str = get_deterministic_string(payload_dict)
        signature = self.crypto.sign(content_str)
        
        final_payload = {
            "sender_id" : self.node_id,
            "content" : content_str,
            "signature" : signature
        }
        
        try:
            reader, writer = await asyncio.open_connection(
                target_info.get('ip', '127.0.0.1'), target_info['port']
            )
            writer.write(json.dumps(final_payload).encode())
            await writer.drain()
            writer.close()
            await writer.wait_closed()

            sender, result = await asyncio.wait_for(future, timeout=3.0)
            
            is_valid = (result == expected_check) if expected_check is not None else True
            
            if is_valid:
                print(f"[VP_CO] Result Verified from {sender}: {result}")
                return True
            else:
                print(f"[VP_CO] Result REJECTED: {result} != {expected_check}")
                return False

        except asyncio.TimeoutError:
            print(f"[VP_CO] Timeout waiting for {target_node_id}")
            return False
        except Exception as e:
            print(f"[VP_CO] Error communicating with {target_node_id}: {e}")
            return False
        finally:
            self.pending_tasks.pop(task_id, None)
            
    async def _send_signed_message_fire_and_forget(self, target_node_id, payload_dict):
        if target_node_id not in self.peers:
            return
            
        target_info = self.peers[target_node_id]
        content_str = get_deterministic_string(payload_dict)
        signature = self.crypto.sign(content_str)
        
        final_payload = {
            "sender_id" : self.node_id,
            "content" : content_str,
            "signature" : signature
        }
        
        try:
            reader, writer = await asyncio.open_connection(
                target_info.get('ip', '127.0.0.1'), target_info['port']
            )
            writer.write(json.dumps(final_payload).encode())
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            print(f"[VP_CO] Failed to send update to {target_node_id}: {e}")
