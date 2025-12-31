import asyncio
import time
import uuid
import logging
from typing import Optional
from .connection_status import ConnectionStatus
from .key_manager import KeyManager
from .encryption_manager import EncryptionManager
from .message import Message
from .message_type import MessageType
from .command_type import CommandType
from .send_receive_msgs import send_message, receive_message

logger = logging.getLogger(__name__)


class ClientState:
    """Self-contained client handler managing its own lifecycle"""
    
    def __init__(self, client_id: str, reader, writer):
        self.client_id = client_id
        self.reader = reader
        self.writer = writer
        self.command_queue = asyncio.Queue()
        self.last_heartbeat = time.time()
        self.status = ConnectionStatus.CONNECTED
        self.pending_results = {}  # {msg_id → result}
        
        # Client-specific encryption management
        self.key_manager = KeyManager()
        self._encryption_manager: Optional[EncryptionManager] = None
    
    def setup_encryption(self, peer_public_key: str) -> str:
        """Setup encryption with peer's public key, returns our public key"""
        session_key = self.key_manager.compute_session_key(peer_public_key)
        self._encryption_manager = EncryptionManager(session_key)
        return self.key_manager.serialized_public_key
    
    def encrypt_message(self, message: Message) -> bytes:
        """Encrypt message for this client"""
        if not self._encryption_manager:
            raise RuntimeError(f"Encryption not setup for client {self.client_id}")
        return self._encryption_manager.encrypt(message)
    
    def decrypt_message(self, encrypted_data: bytes) -> Optional[Message]:
        """Decrypt message from this client"""
        if not self._encryption_manager:
            raise RuntimeError(f"Encryption not setup for client {self.client_id}")
        return self._encryption_manager.decrypt(encrypted_data)
    
    def add_command(self, command: str) -> str:
        """Add command to queue, returns command ID"""
        cmd_id = str(uuid.uuid4())
        self.command_queue.put_nowait({
            "cmd_id": cmd_id,
            "command": command,
        })
        return cmd_id
    
    def kill(self) -> bool:
        """Send kill command to client"""
        try:
            self.pending_results.clear()
            self._cleanup_queue()
            self.add_command(CommandType.KILL.value)
            self.set_killed()
            return True
        except Exception:
            return False
    
    async def run_lifecycle(self):
        """Run the complete client lifecycle"""
        try:
            tasks = [
                asyncio.create_task(self._message_receiver()),
                asyncio.create_task(self._command_executor()),
            ]
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info(f"[{self.client_id}] Lifecycle cancelled")
        except Exception as e:
            logger.error(f"[{self.client_id}] Lifecycle error: {e}")
        finally:
            logger.info(f"[{self.client_id}] Lifecycle finishing")
            await self._cleanup_tasks(tasks)
            await self._cleanup_connection()
    
    async def _message_receiver(self):
        """Receive and process messages from client"""
        try:
            while self.is_connected:
                msg = await receive_message(self.reader)
                if not msg:
                    logger.warning(f"[{self.client_id}] Empty message received")
                    break

                msg = self.decrypt_message(msg)
                if not msg:
                    logger.warning(f"[{self.client_id}] Failed to decrypt message")
                    break
                
                if msg.type is not MessageType.RESULT:
                    logger.warning(f"[{self.client_id}] Unknown message type: {msg.type}")
                    continue
                
                await self._handle_result(msg)
            else:
                logger.info(f'[{self.client_id}] Receiver exiting | client not connected')
        
        except asyncio.CancelledError:
            logger.info(f"[{self.client_id}] Message receiver cancelled")
        except asyncio.IncompleteReadError:
            self._handle_disconnection()
        except Exception as e:
            logger.error(f"[{self.client_id}] Message receiver error: {e}")
    
    async def _command_executor(self):
        """Execute commands from queue"""
        try:
            while self.is_connected:
                cmd_data = await self.command_queue.get()
                if cmd_data is None:
                    logger.info(f'[{self.client_id}] Received empty command from queue')
                    continue
                await self._execute_command(cmd_data)
            else:
                logger.info(f'[{self.client_id}] Executor exiting | client not connected')
        
        except asyncio.CancelledError:
            logger.info(f"[{self.client_id}] Command executor cancelled")
        except Exception as e:
            logger.error(f"[{self.client_id}] Command executor error: {e}")
    
    async def _handle_result(self, msg: Message):
        """Handle result message from client"""
        logger.info(f"[{self.client_id}] Result: {msg.result}")
        if msg.cmd_id in self.pending_results:
            del self.pending_results[msg.cmd_id]
    
    async def _execute_command(self, cmd_data: dict):
        """Execute a single command"""
        cmd_id = cmd_data.get("cmd_id")
        command = cmd_data.get("command")
        logger.info(f"[{self.client_id}] Executing: {command}")

        self.pending_results[cmd_id] = command
        msg = Message.as_command(cmd_id, command)
        encrypted_msg = self.encrypt_message(msg)
        
        await send_message(self.writer, encrypted_msg)
        logger.info(f"[{self.client_id}] Command sent: {command}")
    
    def _handle_disconnection(self):
        """Handle client disconnection"""
        prefix = '[{self.client_id}] Connection closed'
        if self.is_killed:
            logger.info(f"{prefix} - Client killed")
        elif self.is_connected:
            self.set_disconnected()
            self._cleanup_queue()
            self.command_queue.put_nowait(None)  # TODO: add sentinel
            logger.info(f"{prefix} - Client disconnected")
        else:
            logger.warning(f"{prefix} - Client already disconnected")
    
    def _cleanup_queue(self):
        """Clean up command queues"""
        try:
            while not self.command_queue.empty():
                cmd_data = self.command_queue.get_nowait()
                self.command_queue.task_done()
                logger.info(f"[{self.client_id}] Drained command: {cmd_data.get('command', '')}")
        except asyncio.QueueEmpty:
            logger.info(f"[{self.client_id}] Command queue is empty")
    
    async def _cleanup_connection(self):
        """Clean up network connection"""
        if not self.writer or self.writer.is_closing():
            return
        try:
            if self.reader:
                self.reader.feed_eof()
                logger.info(f"[{self.client_id}] Reader feed EOF")
            
            self.writer.close()
            await asyncio.wait_for(self.writer.wait_closed(), timeout=1.0)
            logger.info(f"[{self.client_id}] Connection closed")
        except asyncio.TimeoutError:
            logger.info(f"[{self.client_id}] Timeout closing connection")
        except Exception as e:
            logger.error(f"[{self.client_id}] Error closing connection: {e}")
        finally:
            self.reader = None
            self.writer = None
            logger.info(f"[{self.client_id}] Closed writer for killed client")
    
    async def _cleanup_tasks(self, tasks):
        """Cancel and cleanup tasks"""
        logger.info(f"[{self.client_id}] Cancelling {len(tasks)} tasks")
        for task in tasks:
            if not task.done():
                logger.info(f"[{self.client_id}] Cancelling task: {task.get_name()}")
                task.cancel()
        logger.info(f"[{self.client_id}] Tasks cancelled")
    
    def set_killed(self) -> None:
        self.status = ConnectionStatus.KILLED

    def set_disconnected(self) -> None:
        self.status = ConnectionStatus.DISCONNECTED

    @property
    def is_connected(self) -> bool:
        return self.status is ConnectionStatus.CONNECTED
    
    @property
    def is_killed(self) -> bool:
        return self.status is ConnectionStatus.KILLED
    
    @property
    def is_disconnected(self) -> bool:
        return self.status in (
            ConnectionStatus.DISCONNECTED,
            ConnectionStatus.KILLED,
        )