# roles/executor.py
import asyncio
import json
from core.node import Node
from utils.transfer_to_string import get_deterministic_string
from core.state import VersionedKVStore
from core.transaction import TransactionEngine
from core.types import MessageType

class ExecutorNode(Node):
    def __init__(self, node_id, crypto_manager, config_path = None):
        if config_path:
            super().__init__(node_id, config_path)
        else:
            super().__init__(node_id)
        self.crypto = crypto_manager
        self.store = VersionedKVStore()
        self.tx_engine = TransactionEngine()

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
        payload = msg_data.get('payload', msg_data) 
        msg_id = msg_data.get('id')                 
        sender = msg_data.get('sender', sender_id if 'signature' in message else None)

        if getattr(self, 'is_bad', False) and msg_type in [MessageType.COMPUTE_TASK, "COMPUTE_TASK"]:
            print(f"[{self.node_id}] I am a BAD node. Dropping task to cause a timeout.")
            return {"status": "dropped"}

        if msg_type in [MessageType.STATE_UPDATE, "STATE_UPDATE"]:
            await self.handle_state_update(payload)
            return {"status": "processed"}

        elif msg_type in [MessageType.COMPUTE_TASK, "COMPUTE_TASK"]:
            await self.handle_compute_task(payload, msg_id, sender)
            return {"status": "processing"}
        
        return await super().process_message(message)

    async def handle_state_update(self, payload):
        seq = payload.get('seq')
        tx = payload.get('tx')
        try:
            self.tx_engine.execute(tx, mode='write', state=self.store)
            self.store.commit_version(seq)
            print(f"   [{self.node_id}] Applied Seq {seq} for TX: {tx.get('op')}")
        except Exception as e:
            print(f"[{self.node_id}] Update Failed: {e}")

    async def handle_compute_task(self, payload, request_id, sender):
        task_id = payload.get('task_id')
        tx = payload.get('tx')
        version = payload.get('version', 0)
        
        snapshot = self.store.get_version(version)

        try:
            result = self.tx_engine.execute(tx, mode='read', state=snapshot)

            response_payload = {
                "task_id": task_id,
                "result": result,
                "node_id": self.node_id
            }

            await self._send_secure_reply(sender, request_id, response_payload)

        except Exception as e:
            print(f"[{self.node_id}] Compute Failed: {e}")

    async def _send_secure_reply(self, target_id, request_id, response_payload):
        reply_msg = {
            "type": "RESULT",
            "payload": response_payload,
            "reply_to": request_id,
            "sender": self.node_id,
            "task_id": response_payload["task_id"], 
            "result": response_payload["result"]    
        }
        
        content_str = get_deterministic_string(reply_msg)
        signature = self.crypto.sign(content_str)
        
        final_envelope = {
            "sender_id": self.node_id,
            "signature": signature,
            "content": content_str
        }
        
        if hasattr(self, 'send_message'):
            await self.send_message(target_id, final_envelope)
        else:
            try:
                target_info = self.peers.get(target_id)
                if target_info:
                    target_ip = target_info.get('ip', '127.0.0.1')
                    target_port = target_info.get('port')
                    
                    reader, writer = await asyncio.open_connection(target_ip, target_port)
                    writer.write(json.dumps(final_envelope).encode())
                    await writer.drain()
                    writer.close()
                    await writer.wait_closed()
                else:
                    print(f"[{self.node_id}] Cannot find peer info for {target_id}")
            except Exception as e:
                print(f"[{self.node_id}] Failed to send secure result: {e}")

    def execute_code(self, code, args):
        try:
            local_scope = {"args": args}
            exec(code, {}, local_scope)
            return local_scope.get("output")
        except Exception as e:
            print(f"[{self.node_id}] Execution Error: {e}")
            return None

    async def send_result(self, host, port, task_id, result, expected_check):
        
        result_msg = {
            "type": "RESULT",
            "sender": self.node_id,
            "task_id": task_id,
            "result": result,
            "expected_check": expected_check
        }
        
        content_str = get_deterministic_string(result_msg)
        signature = self.crypto.sign(content_str)
        
        final_payload = {
            "sender_id" : self.node_id,
            "signature" : signature,
            "content" : content_str
        }
        
        try:
            target_ip = host if host else self.ip
            
            reader, writer = await asyncio.open_connection(target_ip, port)
            writer.write(json.dumps(final_payload).encode())
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            print(f"[{self.node_id}] Failed to send result: {e}")
