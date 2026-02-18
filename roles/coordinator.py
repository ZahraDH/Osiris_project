# roles/coordinator.py
import asyncio
import json
import uuid
import time
from core.node import Node

class CoordinatorNode(Node):
    def __init__(self, node_id):
        super().__init__(node_id)
        self.pending_tasks = {}

    async def process_message(self, message):
        msg_type = message.get("type")
        
        if msg_type == "RESULT":
            task_id = message.get("task_id")
            result = message.get("result")
            sender = message.get("sender")
            
            if task_id in self.pending_tasks:
                future = self.pending_tasks[task_id]
                if not future.done():
                    future.set_result((sender, result))
            
            return {"status": "received"}
            
        return await super().process_message(message)

    async def submit_task_with_retry(self, target_nodes, code, args, expected_check=None):
        task_id = str(uuid.uuid4())[:8]
        
        for node_id in target_nodes:
            print(f"\n[VP_CO] Attempting to assign Task {task_id} to {node_id}...")
            
            success = await self._send_task_and_wait(node_id, task_id, code, args, expected_check)
            
            if success:
                print(f"[VP_CO] Task {task_id} completed successfully by {node_id}.")
                return
            else:
                print(f"[VP_CO] Task {task_id} failed on {node_id}. Re-assigning...")
        
        print(f"[VP_CO] CRITICAL: Task {task_id} failed on ALL available nodes.")

    async def _send_task_and_wait(self, target_node_id, task_id, code, args, expected_check):
        if target_node_id not in self.peers:
            return False

        target_info = self.peers[target_node_id]
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.pending_tasks[task_id] = future

        task_msg = {
            "type": "TASK",
            "task_id": task_id,
            "code": code,
            "args": args,
            "reply_host": self.ip,
            "reply_port": self.port,
            "expected_check": expected_check
        }

        try:
            reader, writer = await asyncio.open_connection(
                target_info.get('ip', '127.0.0.1'), target_info['port']
            )
            writer.write(json.dumps(task_msg).encode())
            await writer.drain()
            writer.close()
            await writer.wait_closed()

            sender, result = await asyncio.wait_for(future, timeout=3.0)
            is_valid = (result == expected_check) if expected_check is not None else True
            
            if is_valid:
                print(f"[VP_CO] Result Verified: {result}")
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
