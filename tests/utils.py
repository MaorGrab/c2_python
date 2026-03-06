import time
import asyncio

async def poll_until(condition_func, timeout=5.0):
    """Polls a condition function until it returns True or times out."""
    start = time.time()
    while time.time() - start < timeout:
        if condition_func():
            return True
        await asyncio.sleep(0.1)
    return False