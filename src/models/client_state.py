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
        self.status = ConnectionStatus.CONNECTING
        self.pending_results = {}  # {msg_id → result}

    def set_connected(self) -> None:
        self.status = ConnectionStatus.CONNECTED

    def set_killed(self) -> None:
        self.status = ConnectionStatus.KILLED

    @property
    def is_connected(self) -> bool:
        return self.status is ConnectionStatus.CONNECTED
    
    @property
    def is_killed(self) -> bool:
        return self.status is ConnectionStatus.KILLED