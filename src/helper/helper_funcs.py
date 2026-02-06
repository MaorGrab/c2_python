import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def drain_queue(queue: asyncio.Queue):
    try:
        while not queue.empty():
            data = queue.get_nowait()
            queue.task_done()
            logger.info(f"Drained command id: {data.get('cmd_id', '?')}")
        logger.info("Drained queue")
    except asyncio.QueueEmpty:
        logger.info("Queue is empty")
    except Exception as e:
        logger.error(f"Error draining queue: {e}")

async def cancel_task(task: asyncio.Task, related_queue: Optional[asyncio.Queue] = None):
    # TODO: QUEUE NOT CLEARED + CHECK IF WAIT_FOR LOGIC IS GOOD
    try:
        if related_queue and not related_queue.empty():
            await asyncio.wait_for(task, timeout=2.0)  # TODO: set time var
        if not task.done():
            task.cancel()
            logger.info(f"Task \"{task.get_name()}\" cancelled")
    except asyncio.CancelledError:
        logger.info(f"Task \"{task.get_name()}\" already cancelled")
    except asyncio.TimeoutError:
        logger.info(f"Task \"{task.get_name()}\" timeout during cancellation")  
    except Exception as e:
        logger.error(f"Error cancelling tasks: {e}")