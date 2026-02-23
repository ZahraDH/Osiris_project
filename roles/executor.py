# roles/executor.py
import asyncio
import json
from core.node import Node
from utils.transfer_to_string import get_deterministic_string
from core.state import VersionedKVStore
from core.transaction import TransactionEngine
from core.types import MessageType
from config import NETWORK_CONFIG

class ExecutorNode(Node):
    def __init__(self, node_id, crypto_manager, config_path=None, f=1):
        if config_path:
            super().__init__(node_id, config_path)
        else:
            super().__init__(node_id)
        
        self.crypto = crypto_manager
        self.store = VersionedKVStore()
        self.tx_engine = TransactionEngine()
        self.f = f
        self.quorum_size = f + 1
        self.assignment_buffer = {}
        self.executed_tasks = set()

    async def process_message(self, message):
        if 'signature' in message:
            sender_id = message.get('sender_id')
            signature = message.get('signature')
            content_str = message.get('content')
            
            is_valid = self.crypto.verify(sender_id, content_str, signature)
            if not is_valid:
                print(f"[{self.node_id}] Invalid signature from {sender_id}. Ignoring message.")
                return {"status": "error"}
            
            try:
                msg_data = json.loads(content_str)
            except json.JSONDecodeError:
                print(f"[{self.node_id}] Failed to parse content string.")
                return {"status": "error"}
        else:
            msg_data = message

        msg_type = msg_data.get('type')
        if getattr(self, 'is_bad', False) and msg_type in ["TASK_ASSIGNMENT"]:
            print(f"[{self.node_id}] I am a BAD node. Dropping assignment.")
            return {"status": "dropped"}
        if msg_type in [MessageType.STATE_UPDATE, "STATE_UPDATE"]:
            await self.handle_state_update(msg_data)
            return {"status": "processed"}
        elif msg_type == "TASK_ASSIGNMENT":
            sender = msg_data.get('sender_id', sender_id if 'signature' in message else None)
            await self.handle_task_assignment(msg_data, sender)
            return {"status": "processing"}
        
        return await super().process_message(message)

    async def handle_state_update(self, payload):
        seq = payload.get('seq')
        tx = payload.get('tx')
        try:
            self.tx_engine.execute(tx, mode='write', state=self.store)
            self.store.commit_version(seq)
            print(f"   [{self.node_id}] Applied State Update Seq {seq}: {tx.get('op')}")
        except Exception as e:
            print(f"[{self.node_id}] Update Failed: {e}")

    async def handle_task_assignment(self, assignment_data, sender_id):
        task_id = assignment_data.get('task_id')
        if task_id in self.executed_tasks:
            return

        if task_id not in self.assignment_buffer:
            self.assignment_buffer[task_id] = {}
            
        self.assignment_buffer[task_id][sender_id] = assignment_data
        
        collected_signatures = len(self.assignment_buffer[task_id])
        print(f"[{self.node_id}] Collected {collected_signatures}/{self.quorum_size} assignments for Task {task_id}")

        if collected_signatures >= self.quorum_size:
            if self._verify_assignment_consistency(task_id):
                print(f"[{self.node_id}] Quorum reached for Task {task_id}! Starting execution...")
                self.executed_tasks.add(task_id)
                
                asyncio.create_task(self.execute_and_stream(task_id))
            else:
                print(f"[{self.node_id}] BFT ALERT: Inconsistent assignments received for Task {task_id}!")

    def _verify_assignment_consistency(self, task_id):
        assignments = list(self.assignment_buffer[task_id].values())
        first_tx_str = json.dumps(assignments[0]["tx"], sort_keys=True)
        first_version = assignments[0]["version"]
        
        for assign in assignments[1:]:
            current_tx_str = json.dumps(assign["tx"], sort_keys=True)
            if current_tx_str != first_tx_str or assign["version"] != first_version:
                return False
        return True

    async def execute_and_stream(self, task_id):
        if self.node_id == "EP_Bad":  
            print(f"\n [{self.node_id}] SIMULATING SLOW NODE: Freezing for 10 seconds to trigger timeout...\n")
            await asyncio.sleep(10)
        assignment = list(self.assignment_buffer[task_id].values())[0]
        tx = assignment["tx"]
        version = assignment.get("version") 
        
        snapshot = self.store.get_version(version) if version is not None else {}
        if not snapshot:
            snapshot = getattr(self.store, '_state', {})
            
        print(f"[DEBUG EXECUTOR] Task {task_id} | State used for SUM: {snapshot}")

        try:
            results_iterator = self.tx_engine.execute_streaming(tx, state=snapshot)
            if isinstance(results_iterator, dict):
                results = [results_iterator]
            elif isinstance(results_iterator, (int, float, str)):
                results = [results_iterator]
            elif results_iterator is None:
                results = []
            else:
                results = list(results_iterator)

            chunk = []
            chunk_index = 0
            
            for record in results:
                chunk.append(record)
                if len(chunk) >= 100:  
                    await self._broadcast_chunk_to_verifiers(task_id, chunk, chunk_index, is_final=False)
                    chunk = []
                    chunk_index += 1
            
            await self._broadcast_chunk_to_verifiers(task_id, chunk, chunk_index, is_final=True)
            print(f"[{self.node_id}] Successfully streamed all chunks for Task {task_id}")
        
        except Exception as e:
            print(f"[{self.node_id}] Compute Failed for Task {task_id}: {e}")
        finally:
            self.assignment_buffer.pop(task_id, None)




    async def _broadcast_chunk_to_verifiers(self, task_id, chunk, chunk_index, is_final):
        payload = {
            "type": "COMPUTATION_CHUNK",
            "task_id": task_id,
            "chunk_index": chunk_index,
            "chunk_data": chunk,
            "is_final": is_final,
            "sender": self.node_id,
            "executor_id": self.node_id,
            "node_id": self.node_id
        }
        msg = self.create_message(MessageType.CHUNK_RESULT, payload)
        
        verifiers = [p for p in self.peers if p.startswith("VP")]
        
        for vp in verifiers:
            await self.send_message(vp, msg)


    async def _send_signed_message_fire_and_forget(self, target_node_id, payload_dict):
        if target_node_id not in self.peers:
            return
            
        target_info = self.peers[target_node_id]
        
        content_str = get_deterministic_string(payload_dict)
        signature = self.crypto.sign(content_str)
        
        final_payload = {
            "sender_id": self.node_id,
            "signature": signature,
            "content": content_str
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
            pass
