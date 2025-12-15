import asyncio
import time
from .connection_status import ConnectionStatus

class ClientState:
    """Tracks state for each connected client"""
    
    def __init__(self, client_id: str, reader, writer):
        self.client_id = client_id
        self.reader = reader
        self.writer = writer
        self.command_queue = asyncio.Queue()
        self.last_heartbeat = time.time()
        self.status = ConnectionStatus.CONNECTED
        self.pending_results = {}  # {msg_id → result}

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
    def is_killed(self) -> bool:
        return self.status is ConnectionStatus.KILLED