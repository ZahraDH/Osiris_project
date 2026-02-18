# test_phase1.py
import asyncio
from core.node import OsirisNode

async def main():
    vp_node = OsirisNode("VP_CO")    
    ep_node = OsirisNode("EP_1")

    t1 = asyncio.create_task(vp_node.start())
    t2 = asyncio.create_task(ep_node.start())

    print("Wait for servers to start...")
    await asyncio.sleep(2)

    print("\n--- TEST: Sending Message from VP_CO to EP_1 ---")
    await vp_node.send_message("EP_1", "GREETING", {"text": "Hello Osiris!"})

    await asyncio.sleep(1)
    
    print("\n--- TEST: Sending INVALID Message (Man-in-the-Middle Attack) ---")
    fake_msg = {
        "text": "I am hacker!",
    }
    t1.cancel()
    t2.cancel()
    print("\nTest Finished.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
