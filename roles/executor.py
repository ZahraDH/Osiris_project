# roles/executor.py
import asyncio
import json
from core.node import Node

class ExecutorNode(Node):
    def __init__(self, node_id):
        super().__init__(node_id)

    async def process_message(self, message):
        msg_type = message.get("type")
        if msg_type == "TASK":
            task_id = message.get("task_id")
            print(f"[{self.node_id}] Received Task {task_id}")
            
            code = message.get("code")
            args = message.get("args")
            reply_host = message.get("reply_host")
            reply_port = message.get("reply_port")
            expected_check = message.get("expected_check")
            result = self.execute_code(code, args)
            await self.send_result(reply_host, reply_port, task_id, result, expected_check)
            
            return {"status": "processing"}
        
        return await super().process_message(message)

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
        
        try:
            target_ip = host if host else self.ip
            
            reader, writer = await asyncio.open_connection(target_ip, port)
            writer.write(json.dumps(result_msg).encode())
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            print(f"[{self.node_id}] Failed to send result: {e}")
