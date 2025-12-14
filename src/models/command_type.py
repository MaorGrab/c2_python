"""
Command Type Enum for C2 Protocol Commands
"""

from enum import Enum


class CommandType(Enum):
    """C2 Protocol Command Types"""
    LIST = "list"
    EXIT = "exit"
    KILL = "kill"
    RUN = "run"
    HELP = "help"
    
    def __str__(self) -> str:
        return self.value