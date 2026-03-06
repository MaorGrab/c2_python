"""
Message Type Enum for C2 Protocol
"""

from enum import Enum


class MessageType(Enum):
    """C2 Protocol Message Types"""
    REGISTER = "register"
    ACK = "ack"
    COMMAND = "command"
    RESULT = "result"
    
    def __str__(self) -> str:
        return self.value