# core/network.py
import asyncio
from core.types import Message

class NetworkManager:
    def __init__(self, host, port, on_message_callback):
        self.host = host
        self.port = port
        self.callback = on_message_callback
        self.server = None

    async def start(self):
        self.server = await asyncio.start_server(
            self.handle_client, self.host, self.port
        )
        print(f"[*] Network listening on {self.host}:{self.port}")
        asyncio.create_task(self.server.serve_forever())

    async def handle_client(self, reader, writer):
        data = await reader.read(8192) 
        addr = writer.get_extra_info('peername')

        if data:
            try:
                msg = Message.deserialize(data)
                if msg:
                    await self.callback(msg)
            except Exception as e:
                print(f"Network Error decoding from {addr}: {e}")

        writer.close()
        await writer.wait_closed()

    async def send(self, target_ip, target_port, message_bytes):
        try:
            reader, writer = await asyncio.open_connection(target_ip, target_port)
            writer.write(message_bytes)
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return True
        except ConnectionRefusedError:
            print(f"[!] Connection refused to {target_ip}:{target_port}")
            return False
        except Exception as e:
            print(f"[!] Send error: {e}")
            return False
