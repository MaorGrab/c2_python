"""
C2 Message Protocol
Defines message structure for client-server communication
"""

import json
from typing import Optional, Dict, Any
from dataclasses import dataclass, asdict


@dataclass
class Message:
    """C2 Protocol Message"""
    type: str
    client_id: Optional[str] = None
    cmd_id: Optional[str] = None
    command: Optional[str] = None
    result: Optional[str] = None
    exec_time_ms: Optional[float] = None
    
    def to_json(self) -> str:
        """Serialize to JSON string"""
        # Filter out None values
        data = {k: v for k, v in asdict(self).items() if v is not None}
        return json.dumps(data)

    def to_payload(self) -> bytes:
        message = self.to_json().encode()
        prefix = len(message).to_bytes(4, 'big')
        return prefix + message
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Message':
        """Deserialize from JSON string"""
        data = json.loads(json_str)
        return cls(**data)
    
    @classmethod
    def from_payload(cls, payload: bytes) -> 'Message':
        """Deserialize from payload"""
        payload = payload.decode()
        return cls.from_json(payload) if payload else None
    
    @classmethod
    def as_register(cls, client_id: str) -> 'Message':
        """Create registration message"""
        return cls(type="register", client_id=client_id)
    
    @classmethod
    def as_ack(cls, client_id: str) -> 'Message':
        """Create acknowledgment message"""
        return cls(type="ack", client_id=client_id)
    
    @classmethod
    def as_command(cls, cmd_id: str, command: str) -> 'Message':
        """Create command message"""
        return cls(type="command", cmd_id=cmd_id, command=command)
    
    @classmethod
    def as_result(cls, cmd_id: str, result: str, exec_time_ms: float) -> 'Message':
        """Create result message"""
        return cls(type="result", cmd_id=cmd_id, result=result, exec_time_ms=exec_time_ms)
