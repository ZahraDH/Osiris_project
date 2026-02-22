import asyncio
from roles.executor import ExecutorNode
from core.types import MessageType

class MaliciousExecutor(ExecutorNode):
    def __init__(self, node_id, crypto_manager, config_path=None):
        super().__init__(node_id, crypto_manager, config_path)
        print(f"[{self.node_id}] Started (MALICIOUS MODE)")

    async def handle_compute_task(self, payload, request_id, sender):
        task_id = payload['task_id']
        fake_result = -999999

        print(f"[{self.node_id}] Received Task {task_id[:6]}. Returning FAKE result: {fake_result}")

        response_payload = {
            "task_id": task_id,
            "result": fake_result,  
            "node_id": self.node_id
        }
        await self._send_secure_reply(sender, request_id, response_payload)
