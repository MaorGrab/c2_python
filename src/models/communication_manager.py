import asyncio
import logging
from typing import Optional
from models.message import Message
from models.send_receive_msgs import send_message, receive_message
from models.message_type import MessageType
from models.command_type import CommandType
from models.encryption_manager import EncryptionManager
from models.connection_manager import ConnectionManager
import helper.helper_funcs as helper_funcs

logger = logging.getLogger(__name__)


class CommunicationManager:
    """Handles message-level I/O, encryption, and queue management"""
    
    def __init__(self, connection_manager: ConnectionManager):
        self.connection_manager = connection_manager
        self.encryption_manager = None
        self.command_queue = asyncio.Queue()
        self.result_queue = asyncio.Queue()
        self._active_event = asyncio.Event()
        self._listener_task = None
        self._talker_task = None
        self._should_stop = False

    @property
    def is_active(self):
        """Check if communication is active"""
        return self._active_event.is_set() and not self._should_stop
    
    async def wait_active(self):
        """Wait until communication is active"""
        await self._active_event.wait()

    async def start_communication(self):
        """Start listener and talker tasks"""
        if self._should_stop:
            logger.info("Communication manager stopped, cannot start")
            return
        
        # Wait for connection to be ready
        await self.connection_manager.wait_connected()
            
        self._listener_task = asyncio.create_task(self._listener(), name='listener')
        self._talker_task = asyncio.create_task(self._talker(), name='talker')
        self._active_event.set()
        
        try:
            await self._listener_task
        except asyncio.CancelledError:
            logger.info("Communication detected cancellation")
            raise
        except asyncio.IncompleteReadError:
            logger.info("Communication detected server closed connection")
            raise
        except Exception as e:
            logger.error(f"Communication error: {e}")
            raise
        finally:
            await self._cleanup_tasks()

    async def _cleanup_tasks(self):
        """Cleanup tasks and go dormant"""
        self._active_event.clear()
        await helper_funcs.cancel_task(self._talker_task, related_queue=self.result_queue)
        logger.info("Communication manager dormant")

    async def _listener(self):
        """Listen for incoming commands from server"""
        try:
            while not self._should_stop:
                encrypted_message = await receive_message(self.connection_manager.reader)
                if not encrypted_message:
                    logger.warning("Received empty message")
                    break
                    
                msg = self.encryption_manager.decrypt(encrypted_message)
                if not msg:
                    logger.warning("Failed to decrypt message")
                    break
                
                if msg.type is not MessageType.COMMAND:
                    logger.warning(f"Unknown message type: {msg.type}")
                    continue

                logger.info(f"Received command: {msg.command}, id: {msg.cmd_id}")
                
                if msg.command == CommandType.KILL.value:
                    if await self._handle_kill_command():
                        break
                    continue
                    
                await self.command_queue.put({
                    "cmd_id": msg.cmd_id,
                    "command": msg.command
                })
            else:
                logger.info("Listener exiting | stopped")
        
        except asyncio.CancelledError:
            logger.info('Listener cancelled')
            raise
        except asyncio.IncompleteReadError:
            logger.info("Server closed connection")
            raise
        except Exception as e:
            logger.error(f"Listener error: {e}")
            raise
        finally:
            logger.info("Listener finished")

    async def _talker(self):
        """Send results back to server"""
        try:
            while not self._should_stop:
                result_data = await self.result_queue.get()
                command_id, result, execution_time_ms = result_data
                
                msg = Message.as_result(command_id, result, execution_time_ms)
                encrypted_msg = self.encryption_manager.encrypt(msg)
                
                if await send_message(self.connection_manager.writer, encrypted_msg):
                    logger.info(f"[cmd id: {command_id}] Result sent ({execution_time_ms:.1f}ms)")
                else:
                    logger.warning(f"[cmd id: {command_id}] Failed to send result")
            else:
                logger.info("Talker exiting | stopped")
                
        except asyncio.CancelledError:
            logger.info("Talker cancelled")
            raise
        except Exception as e:
            logger.error(f"Talker error: {e}")
            raise
        finally:
            logger.info("Talker finished")

    async def _handle_kill_command(self):
        """Handle KILL command by draining queues and breaking listener loop"""
        logger.info('Received KILL command')
        logger.info('Draining command queue')
        helper_funcs.drain_queue(self.command_queue)
        logger.info('Draining result queue')
        helper_funcs.drain_queue(self.result_queue)
        return True

    async def send_result(self, cmd_id: str, result: str, exec_time_ms: float):
        """Enqueue result for sending"""
        await self.result_queue.put((cmd_id, result, exec_time_ms))

    async def receive_command(self) -> Optional[tuple[str, str]]:
        """Dequeue command for execution"""
        try:
            command_data = await self.command_queue.get()
            if command_data is None:
                return None
            return command_data.get("cmd_id"), command_data.get("command", "").lower()
        except asyncio.CancelledError:
            return None

    def stop(self):
        """Stop communication permanently"""
        self._should_stop = True
        self._active_event.clear()

    async def perform_handshake(self, client_id: str) -> bool:
        """Perform registration and encryption setup"""
        try:
            # Create encryption manager with key exchange
            self.encryption_manager = EncryptionManager()
            
            # Send registration with our public key
            msg = Message.as_register(client_id, self.encryption_manager.public_key_b64)
            if not await send_message(self.connection_manager.writer, msg.to_payload(True)):
                logger.error("Failed to send registration")
                return False
            
            # Receive ACK with server's public key
            ack_payload = await receive_message(self.connection_manager.reader)
            if not ack_payload:
                logger.error("No acknowledgment received")
                return False
            
            ack_msg = Message.from_payload(ack_payload)
            if not ack_msg or ack_msg.type is not MessageType.ACK:
                logger.error(f"Invalid acknowledgment: {ack_msg}")
                return False
            
            # Establish session key from server's public key
            self.encryption_manager.establish_session_key(ack_msg.command)
            logger.info(f"Handshake complete for {ack_msg.client_id}")
            return True
            
        except Exception as e:
            logger.error(f"Handshake failed: {e}")
            return False
