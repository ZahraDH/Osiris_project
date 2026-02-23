import asyncio
import json
from core.node import Node
from core.types import MessageType
from config import NETWORK_CONFIG

class Client(Node):
    def __init__(self, node_id, config_path):
        super().__init__(node_id, config_path)

    async def submit_transaction(self, tx_payload, targets=None):
        if targets is None:
            targets = ["CO_1"]

        msg = self.create_message(MessageType.CLIENT_REQUEST, tx_payload)
        print(f"[Client] Submitting: {json.dumps(tx_payload)} to {targets}")
        
        for target in targets:
            if target in self.peers:
                await self.send_message(target, msg)
            else:
                print(f"[Client] Warning: Target {target} is not in peers list!")


    async def process_message(self, message):
        msg_data = message        
        if isinstance(message, dict) and 'content' in message:
            try:
                msg_data = json.loads(message['content'])
            except json.JSONDecodeError:
                pass

        if isinstance(msg_data, dict):
            msg_type = msg_data.get("type", "")            
            if msg_type == "STATE_UPDATE" or msg_type == getattr(MessageType, 'STATE_UPDATE', 'STATE_UPDATE'):
                return {"status": "received"}

        return await super().process_message(message)


    async def handle_message(self, message, reader, writer):
        pass
    
    async def submit_task_to_quorum(self, tx):
        tasks = []
        for co_node in NETWORK_CONFIG["VP_CO"]:
            tasks.append(self.network.send(
                address=(co_node["host"], co_node["port"]),
                message={"type": "NEW_TASK", "tx": tx, "client_id": self.id}
            ))
        await asyncio.gather(*tasks)
        print("Task broadcasted to VP_CO sub-cluster.")
