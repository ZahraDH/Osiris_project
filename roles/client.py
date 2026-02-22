# roles/client.py

import asyncio
import json
from core.node import Node
from core.types import MessageType


class Client(Node):
    def __init__(self, node_id, config_path):
        super().__init__(node_id, config_path)

    async def submit_transaction(self, tx_payload):
        coordinators = [nid for nid in self.peers if nid.startswith("VP")]
        if not coordinators: return

        target = coordinators[0]
        msg = self.create_message(MessageType.CLIENT_REQUEST, tx_payload)

        print(f"[Client] Submitting: {json.dumps(tx_payload)}")
        await self.send_message(target, msg)

    async def handle_message(self, message, reader, writer):
        pass
