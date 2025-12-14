import logging 
import asyncio
from message import Message

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def send_message(writer, message: Message) -> bool:
    """
    Send JSON message to client
    Format: length_prefix(4 bytes)
    """
    try:        
        writer.write(message.to_payload())
        await writer.drain()
        return True
    except Exception as e:
        logger.error(f"Failed to send message: {e}")
        return False

async def receive_message(reader) -> Message:
    """
    Receive JSON message from client
    """
    try:
        # Read length prefix
        length_data = await reader.readexactly(4)
        length = int.from_bytes(length_data, 'big')
        
        # Read payload
        payload = await reader.readexactly(length)
        return Message.from_payload(payload)
    except asyncio.IncompleteReadError:
        logger.info("Connection closed")
        return None
    except ConnectionResetError:
        logger.info("Connection reset by peer")
        return None
    except Exception as e:
        logger.error(f"Failed to receive message: {e}")
        return None