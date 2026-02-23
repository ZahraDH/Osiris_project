import asyncio
from roles.executor import ExecutorNode
from core.types import MessageType

class MaliciousExecutor(ExecutorNode):
    def __init__(self, node_id, crypto_manager, config_path=None):
        super().__init__(node_id, crypto_manager, config_path)
        print(f"[{self.node_id}] Started (MALICIOUS MODE)")

    async def execute_and_stream(self, task_id, *args, **kwargs):
        try:
            # print(f"[{self.node_id}] MALICIOUS: Generating FAKE chunk for Task {task_id[:8]}...")
            
            # fake_chunk = [{"result": -999999}]
            # await self._broadcast_chunk_to_verifiers(
            #     task_id=task_id, 
            #     chunk=fake_chunk, 
            #     chunk_index=0, 
            #     is_final=True
            # )
            print(f"\n[EP_Bad] SIMULATING DEAD NODE: Freezing for 10 seconds to force TIMEOUT...\n")
            await asyncio.sleep(10) 
            return
        
        except Exception as e:
            print(f"[{self.node_id}] Malicious Execute Failed: {e}")
