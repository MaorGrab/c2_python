"""
Unit tests for Message protocol
Tests serialization, deserialization, and message creation
"""
import pytest
import json
from models.message import Message
from models.message_type import MessageType


def test_message_to_json():
    """Test message serialization to JSON"""
    msg = Message(type=MessageType.COMMAND, cmd_id="123", command="whoami")
    json_str = msg.to_json()
    data = json.loads(json_str)
    
    assert data["type"] == "command"
    assert data["cmd_id"] == "123"
    assert data["command"] == "whoami"
    assert "result" not in data


def test_message_from_json():
    """Test message deserialization from JSON"""
    json_str = '{"type": "result", "cmd_id": "456", "result": "output", "exec_time_ms": 100.5}'
    msg = Message.from_json(json_str)
    
    assert msg.type == MessageType.RESULT
    assert msg.cmd_id == "456"
    assert msg.result == "output"
    assert msg.exec_time_ms == 100.5


def test_message_to_payload_without_prefix():
    """Test payload creation without length prefix"""
    msg = Message(type=MessageType.ACK, client_id="client-1")
    payload = msg.to_payload(with_prefix=False)
    
    assert isinstance(payload, bytes)


def test_message_to_payload_with_prefix():
    """Test payload creation with length prefix"""
    msg = Message(type=MessageType.ACK, client_id="client-1")
    payload = msg.to_payload(with_prefix=True)
    
    length = int.from_bytes(payload[:4], 'big')
    assert length == len(payload) - 4


def test_message_from_payload():
    """Test message deserialization from payload"""
    original = Message(type=MessageType.COMMAND, cmd_id="789", command="ls")
    payload = original.to_payload(with_prefix=False)
    
    reconstructed = Message.from_payload(payload)
    
    assert reconstructed.type == original.type
    assert reconstructed.cmd_id == original.cmd_id
    assert reconstructed.command == original.command


def test_as_register():
    """Test register message factory"""
    msg = Message.as_register("client-123", "public_key_data")
    
    assert msg.type == MessageType.REGISTER
    assert msg.client_id == "client-123"
    assert msg.command == "public_key_data"


def test_as_ack():
    """Test acknowledgment message factory"""
    msg = Message.as_ack("client-456", "server_public_key")
    
    assert msg.type == MessageType.ACK
    assert msg.client_id == "client-456"
    assert msg.command == "server_public_key"


def test_as_command():
    """Test command message factory"""
    msg = Message.as_command("cmd-001", "whoami")
    
    assert msg.type == MessageType.COMMAND
    assert msg.cmd_id == "cmd-001"
    assert msg.command == "whoami"


def test_as_result():
    """Test result message factory"""
    msg = Message.as_result("cmd-002", "root", 50.25)
    
    assert msg.type == MessageType.RESULT
    assert msg.cmd_id == "cmd-002"
    assert msg.result == "root"
    assert msg.exec_time_ms == 50.25


def test_roundtrip_serialization():
    """Test complete serialization roundtrip"""
    original = Message.as_result("cmd-999", "test output", 123.45)
    json_str = original.to_json()
    reconstructed = Message.from_json(json_str)
    
    assert reconstructed.type == original.type
    assert reconstructed.cmd_id == original.cmd_id
    assert reconstructed.result == original.result
    assert reconstructed.exec_time_ms == original.exec_time_ms
