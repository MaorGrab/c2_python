import asyncio
import time
from typing import Optional
from .connection_status import ConnectionStatus
from .key_manager import KeyManager
from .encryption_manager import EncryptionManager
from .message import Message


class ClientState:
    """Encapsulates all client-specific state and operations"""
    
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

    def set_killing(self) -> None:
        self.status = ConnectionStatus.KILLING
    
    def set_killed(self) -> None:
        self.status = ConnectionStatus.KILLED

    @property
    def is_connected(self) -> bool:
        return self.status in (
            ConnectionStatus.CONNECTED,
            ConnectionStatus.KILLING
        )
    
    @property
    def is_killing(self) -> bool:
        return self.status is ConnectionStatus.KILLING
    
    @property
    def is_killed(self) -> bool:
        return self.status is ConnectionStatus.KILLED