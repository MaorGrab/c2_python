"""
Client Registry for C2 Server
"""

import asyncio
import logging
import time
from typing import Dict, Optional
from models.server.client_state import ClientState
from models.protocol.message import Message
from models.protocol.message_type import MessageType
from models.network.send_receive_msgs import send_message, receive_message

logger = logging.getLogger(__name__)


class ClientManager:
    """Simple client registry - registration, lookup, and coordination only"""
    
    def __init__(self):
        self.clients: Dict[str, ClientState] = {}
    
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
        client = self.get_client(client_id)
        if not client:
            return False
        
        try:
            client.add_command(command)
            return True
        except Exception:
            return False
    
    def kill_client(self, client_id: str) -> bool:
        """Send kill command to client"""
        client = self.get_client(client_id)
        if not client:
            return False
        
        return client.kill()
    
    async def handle_client_loop(self, client_state: ClientState):
        """Delegate to client's own lifecycle management"""
        await client_state.run_lifecycle()
    
    async def close_all_clients(self):
        """Close all client connections"""
        for client_state in list(self.clients.values()):
            await client_state._cleanup_connection()
        self.clients.clear()
    
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