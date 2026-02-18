# core/node.py
import asyncio
import json
import socket

class Node:
    def __init__(self, node_id, config_file='config.json'):
        self.node_id = node_id
        self.config = self.load_config(config_file)
        
        if node_id not in self.config['nodes']:
            raise ValueError(f"Node ID {node_id} not found in {config_file}")
            
        node_info = self.config['nodes'][node_id]
        self.ip = node_info.get('ip', node_info.get('host', '127.0.0.1'))
        self.port = int(node_info['port'])
        self.peers = self.config['nodes']

    def load_config(self, config_file):
        with open(config_file, 'r') as f:
            return json.load(f)

    async def handle_client(self, reader, writer):
        data = await reader.read(4096)
        try:
            message = json.loads(data.decode())
            response = await self.process_message(message)
            if response:
                writer.write(json.dumps(response).encode())
                await writer.drain()
        except Exception as e:
            print(f"[{self.node_id}] Error handling msg: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def process_message(self, message):
        print(f"[{self.node_id}] Received: {message}")
        return {"status": "ok"}

    async def start(self):
        print(f"--- Node {self.node_id} Starting ---")
        try:
            server = await asyncio.start_server(self.handle_client, self.ip, self.port)
            print(f"[*] {self.node_id} listening on {self.ip}:{self.port}")
            async with server:
                await server.serve_forever()
        except OSError as e:
            if e.errno == 98:
                print(f"[!!!] CRITICAL: Port {self.port} is BUSY. Please kill old processes.")
            else:
                print(f"[!!!] Error starting {self.node_id}: {e}")
