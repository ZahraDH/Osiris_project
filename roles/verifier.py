# roles/verifier.py
import asyncio
import json
from core.node import Node
from core.state import VersionedKVStore
from core.transaction import TransactionEngine
from core.verification import VerificationOperators
from core.types import MessageType
from utils.transfer_to_string import get_deterministic_string

class VerifierNode(Node):
    def __init__(self, node_id, crypto_manager, config_path):
        super().__init__(node_id, config_path)
        self.crypto = crypto_manager
        self.store = VersionedKVStore()
        self.tx_engine = TransactionEngine()
        self.active_verification_sessions = {}

        print(f"[Verifier {self.node_id}] Started. Waiting for assignments...")

    async def process_message(self, message):
        if isinstance(message, dict) and 'signature' in message:
            sender_id = message.get("sender_id")
            signature = message.get("signature")
            content_str = message.get("content")

            if not self.crypto.verify(sender_id, content_str, signature):
                print(f"[{self.node_id}] INVALID SIGNATURE from {sender_id}. Dropping.")
                return {"status": "error"}

            try:
                msg_data = json.loads(content_str)
            except:
                return {"status": "error"}
        else:
            msg_data = message

        if not isinstance(msg_data, dict):
            return {"status": "error"}

        msg_type = msg_data.get("type")


        if msg_type == MessageType.STATE_UPDATE or msg_type == "STATE_UPDATE":
            await self.handle_state_update(msg_data)
            return {"status": "received"} 

        elif msg_type == MessageType.TASK_ASSIGNMENT or msg_type == "TASK_ASSIGNMENT":
            await self.handle_assignment(msg_data, message.get("signature") if isinstance(message, dict) else None)
            return {"status": "received"}  

        elif msg_type == MessageType.CHUNK_RESULT or msg_type == "CHUNK_RESULT":
            sender_name = message.get("sender_id") or msg_data.get("sender") or msg_data.get("node_id") or "Unknown"
            await self.handle_result_chunk(msg_data, sender_name)
            return {"status": "received"}  
        
        return await super().process_message(message)


    async def handle_state_update(self, payload):
        seq = payload.get("seq")
        tx = payload.get("tx")
        try:
            self.tx_engine.execute(tx, mode='write', state=self.store)
            self.store.commit_version(seq)
        except Exception as e:
            print(f"[{self.node_id}] Sync Error: {e}")

    async def handle_assignment(self, payload, coordinator_signature):
        task_id = payload.get("task_id")
        tx = payload.get("tx")
        executor_id = payload.get("executor_id")
        version = payload.get("version")

        safe_task_id = str(task_id)[:8] if task_id else "UNKNOWN"
        print(f"[{self.node_id}] Assigned to verify Task {safe_task_id} from {executor_id}")

        self.active_verification_sessions[task_id] = {
            "tx": tx,
            "version": version,
            "executor_id": executor_id,
            "proof": coordinator_signature,
            "received_chunks": [], 
            "seen_records": 0,     
            "status": "pending"
        }

    async def handle_result_chunk(self, payload, sender_id):
        
        task_id = payload.get("task_id")
        
        chunk_data = payload.get("chunk_data", payload.get("chunk", payload.get("data", [])))
        is_final = payload.get("is_final", False)    
        
        if not task_id and "payload" in payload and isinstance(payload["payload"], dict):
            inner = payload["payload"]
            task_id = inner.get("task_id", task_id)
            chunk_data = inner.get("chunk_data", inner.get("chunk", inner.get("data", chunk_data)))
            is_final = inner.get("is_final", is_final)

        if not chunk_data and "result" in payload:
            chunk_data = payload.get("result", [])

        safe_task_id = str(task_id)[:8] if task_id else "UNKNOWN"
        is_fraud = False            
        if chunk_data == -999999 or chunk_data == "-999999":
            is_fraud = True

        if is_fraud:
            print(f"[{self.node_id}] BFT ALERT: FRAUD DETECTED! Rejecting fake chunk from {sender_id} for Task {safe_task_id} 🛑")
            if task_id:
                await self.send_vote(task_id, False, "Malicious BFT Node Detected")
                if task_id in self.active_verification_sessions:
                    del self.active_verification_sessions[task_id]
            return

        if isinstance(chunk_data, dict):
            chunk_data = [chunk_data]
        elif not isinstance(chunk_data, list):
            if chunk_data is not None and chunk_data != "":
                chunk_data = [chunk_data]
            else:
                chunk_data = []

        session = self.active_verification_sessions.get(task_id)

        if not session:
            print(f"🧐 [{self.node_id}] Received chunk for UNKNOWN task {safe_task_id}. Ignoring.")
            return

        if sender_id != session.get("executor_id"):
            print(f"🧐 [{self.node_id}] Unauthorized sender {sender_id} for Task {safe_task_id}.")
            return

        tx = session["tx"]

        is_chunk_valid = VerificationOperators.verify_records_validity(chunk_data, tx)
        if not is_chunk_valid:
            print(f"[{self.node_id}] REJECTED Chunk for {safe_task_id}: Invalid records detected.")
            await self.send_vote(task_id, False, "Invalid record format")
            del self.active_verification_sessions[task_id]
            return

        session["received_chunks"].extend(chunk_data)
        session["seen_records"] += len(chunk_data)

        if is_final:
            expected_size = VerificationOperators.estimate_output_size(tx)
            
            op_type = ""
            if isinstance(tx, dict):
                op_type = tx.get("op", "")
                if not op_type and "task" in tx and isinstance(tx["task"], dict):
                    op_type = tx["task"].get("op", "")
            
            seen = session.get("seen_records", 0)
            AGGREGATION_OPS = ["SUM_ALL", "COUNT", "MAX", "MIN", "AVERAGE"]
            
            if op_type in AGGREGATION_OPS:
                expected_size = 1
            
            print(f"🔍 [DEBUG] Verifier {self.node_id} | Task: {safe_task_id} | Op: '{op_type}' | Seen: {seen} | Expected: {expected_size}")

            
            if expected_size != -1 and seen != expected_size:
                print(f"[{self.node_id}] REJECTED Task {safe_task_id}: Size mismatch. (Seen: {seen}, Expected: {expected_size})")
                await self.send_vote(task_id, False, "Output size mismatch")
            
            elif op_type not in AGGREGATION_OPS and not VerificationOperators.verify_ordering(session["received_chunks"]):
                print(f"[{self.node_id}] REJECTED Task {safe_task_id}: Ordering verification failed.")
                await self.send_vote(task_id, False, "Records are not properly ordered")
                
            else:
                print(f"[{self.node_id}] VERIFIED Task {safe_task_id}. Total: {seen}")
                await self.send_vote(task_id, True, session["received_chunks"])

            if task_id in self.active_verification_sessions:
                del self.active_verification_sessions[task_id]




    async def send_vote(self, task_id, is_valid, result_or_reason):
        """Sends a signed vote back to the Coordinator (VP_CO)"""
        vote_payload = {
            "type": MessageType.VERIFICATION_VOTE,
            "task_id": task_id,
            "verifier_id": self.node_id,
            "is_valid": is_valid,
            "result": result_or_reason
        }

        coordinators = [nid for nid in self.peers if "CO" in nid]
        for co in coordinators:
            await self._send_signed(co, vote_payload)

    async def _send_signed(self, target_id, payload_dict):
        content_str = get_deterministic_string(payload_dict)
        signature = self.crypto.sign(content_str)

        msg = {
            "sender_id": self.node_id,
            "content": content_str,
            "signature": signature
        }
        if hasattr(self, 'network'):
            peer_info = self.peers.get(target_id)
            if peer_info:
                await self.network.send((peer_info.get('ip', '127.0.0.1'), peer_info['port']), msg)
        else:
            await self.send_message(target_id, msg)
