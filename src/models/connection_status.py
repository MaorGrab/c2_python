"""
Connection Status Enum for C2 Client States
"""

from enum import Enum


class ConnectionStatus(Enum):
    """C2 Client Connection Status"""
    CONNECTING = "connecting"
    CONNECTED = "connected"
    KILLED = "killed"
    DISCONNECTED = "disconnected"
    
    def __str__(self) -> str:
        return self.value