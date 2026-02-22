import asyncio
import json
import uuid
from typing import Dict, Optional, Any

class Node:
    def __init__(self, node_id: str, config_path: str = 'config.json'):
        self.node_id = node_id
        self.config_path = config_path
        self.ip: Optional[str] = None
        self.port: Optional[int] = None
        self.peers: Dict[str, Dict[str, Any]] = {}
        self._server: Optional[asyncio.AbstractServer] = None
        self._server_task: Optional[asyncio.Task] = None
        self.pending_responses: Dict[str, asyncio.Future] = {}
        self._load_config()


    def _load_config(self):
        with open(self.config_path, 'r') as f:
            cfg = json.load(f)

        nodes_config = cfg.get('nodes') or cfg.get('peers') or cfg.get('cluster')
        if not nodes_config:
            raise ValueError("Config must contain 'nodes' or 'peers' key")

        if isinstance(nodes_config, list):
            temp_dict = {}
            for entry in nodes_config:
                if isinstance(entry, dict):
                    pid = entry.get('id') or entry.get('name') or entry.get('node_id')
                    if pid:
                        temp_dict[pid] = entry
            nodes_config = temp_dict

        if self.node_id not in nodes_config:
            raise ValueError(f"Node ID {self.node_id} not found in {self.config_path}")

        for pid, entry in nodes_config.items():
            ip = entry.get('ip') or entry.get('host') or entry.get('address') or '127.0.0.1'
            port = int(entry.get('port', 0))

            if pid == self.node_id:
                self.ip = ip
                self.port = port
            else:
                self.peers[pid] = {"ip": ip, "port": port}

    def create_message(self, msg_type: str, payload: dict, reply_to: Optional[str] = None) -> dict:
        msg = {
            "id": str(uuid.uuid4()),
            "type": msg_type,
            "sender": self.node_id,
            "payload": payload,
        }
        if reply_to is not None:
            msg["reply_to"] = reply_to
        return msg

    async def start(self):
        print(f"--- Node {self.node_id} Starting ---")
        try:
            self._server = await asyncio.start_server(self.handle_connection, self.ip, self.port)
            print(f"[*] {self.node_id} listening on {self.ip}:{self.port}")
            self._server_task = asyncio.create_task(self._server.serve_forever())
        except OSError as e:
            if e.errno == 98:
                print(f"[!!!] CRITICAL: Port {self.port} is BUSY. Please kill old processes.")
            else:
                print(f"[!!!] Error starting {self.node_id}: {e}")

    async def stop(self):
        for req_id, fut in list(self.pending_responses.items()):
            if not fut.done():
                fut.cancel()
        self.pending_responses.clear()

        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        if self._server_task is not None:
            self._server_task.cancel()
            try:
                await self._server_task
            except asyncio.CancelledError:
                pass
            self._server_task = None

        print(f"[{self.node_id}] Stopped.")

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            data = await reader.read(4096)
            if not data:
                return

            message = json.loads(data.decode())
            
            reply_to = message.get('reply_to')
            if reply_to and reply_to in self.pending_responses:
                fut = self.pending_responses.pop(reply_to)
                if not fut.done():
                    fut.set_result(message)
            else:
                response = await self.process_message(message)
                if response:
                    writer.write(json.dumps(response).encode())
                    await writer.drain()

        except Exception as e:
            print(f"[{self.node_id}] Error handling connection/message: {e}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def process_message(self, message):
        print(f"[{self.node_id}] Received: {message.get('type', 'UNKNOWN')}")
        return {"status": "ok"}

    async def send_message(self, target_id: str, message: dict):
        target_info = self.peers.get(target_id)
        if not target_info:
            print(f"[{self.node_id}] Unknown peer '{target_id}'")
            return

        host = target_info.get('ip', target_info.get('host', '127.0.0.1'))
        port = target_info.get('port')
        
        try:
            reader, writer = await asyncio.open_connection(host, port)
            writer.write(json.dumps(message).encode())
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            print(f"[{self.node_id}] send_message to {target_id} failed: {e}")

    async def send_request(self, target_id: str, message: dict, timeout: float = 3.0):
        msg_id = message.get('id')
        if not msg_id:
            msg_id = str(uuid.uuid4())
            message['id'] = msg_id

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self.pending_responses[msg_id] = fut

        await self.send_message(target_id, message)

        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            self.pending_responses.pop(msg_id, None)
            print(f"[{self.node_id}] RPC timeout for {msg_id} -> {target_id}")
            return None
