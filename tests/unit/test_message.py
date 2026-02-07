"""
Unit tests for Message protocol
Tests serialization, deserialization, and message creation
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

import unittest
import json
from models.message import Message
from models.message_type import MessageType


class TestMessage(unittest.TestCase):
    """Test Message protocol functionality"""
    
    def test_message_to_json(self):
        """Test message serialization to JSON"""
        msg = Message(type=MessageType.COMMAND, cmd_id="123", command="whoami")
        json_str = msg.to_json()
        data = json.loads(json_str)
        
        self.assertEqual(data["type"], "command")
        self.assertEqual(data["cmd_id"], "123")
        self.assertEqual(data["command"], "whoami")
        self.assertNotIn("result", data)  # None fields excluded
    
    def test_message_from_json(self):
        """Test message deserialization from JSON"""
        json_str = '{"type": "result", "cmd_id": "456", "result": "output", "exec_time_ms": 100.5}'
        msg = Message.from_json(json_str)
        
        self.assertEqual(msg.type, MessageType.RESULT)
        self.assertEqual(msg.cmd_id, "456")
        self.assertEqual(msg.result, "output")
        self.assertEqual(msg.exec_time_ms, 100.5)
    
    def test_message_to_payload_without_prefix(self):
        """Test payload creation without length prefix"""
        msg = Message(type=MessageType.ACK, client_id="client-1")
        payload = msg.to_payload(with_prefix=False)
        
        self.assertIsInstance(payload, bytes)
        self.assertNotIn(b'\x00\x00\x00', payload[:4])  # No length prefix
    
    def test_message_to_payload_with_prefix(self):
        """Test payload creation with length prefix"""
        msg = Message(type=MessageType.ACK, client_id="client-1")
        payload = msg.to_payload(with_prefix=True)
        
        length = int.from_bytes(payload[:4], 'big')
        self.assertEqual(length, len(payload) - 4)
    
    def test_message_from_payload(self):
        """Test message deserialization from payload"""
        original = Message(type=MessageType.COMMAND, cmd_id="789", command="ls")
        payload = original.to_payload(with_prefix=False)
        
        reconstructed = Message.from_payload(payload)
        
        self.assertEqual(reconstructed.type, original.type)
        self.assertEqual(reconstructed.cmd_id, original.cmd_id)
        self.assertEqual(reconstructed.command, original.command)
    
    def test_as_register(self):
        """Test register message factory"""
        msg = Message.as_register("client-123", "public_key_data")
        
        self.assertEqual(msg.type, MessageType.REGISTER)
        self.assertEqual(msg.client_id, "client-123")
        self.assertEqual(msg.command, "public_key_data")
    
    def test_as_ack(self):
        """Test acknowledgment message factory"""
        msg = Message.as_ack("client-456", "server_public_key")
        
        self.assertEqual(msg.type, MessageType.ACK)
        self.assertEqual(msg.client_id, "client-456")
        self.assertEqual(msg.command, "server_public_key")
    
    def test_as_command(self):
        """Test command message factory"""
        msg = Message.as_command("cmd-001", "whoami")
        
        self.assertEqual(msg.type, MessageType.COMMAND)
        self.assertEqual(msg.cmd_id, "cmd-001")
        self.assertEqual(msg.command, "whoami")
    
    def test_as_result(self):
        """Test result message factory"""
        msg = Message.as_result("cmd-002", "root", 50.25)
        
        self.assertEqual(msg.type, MessageType.RESULT)
        self.assertEqual(msg.cmd_id, "cmd-002")
        self.assertEqual(msg.result, "root")
        self.assertEqual(msg.exec_time_ms, 50.25)
    
    def test_roundtrip_serialization(self):
        """Test complete serialization roundtrip"""
        original = Message.as_result("cmd-999", "test output", 123.45)
        json_str = original.to_json()
        reconstructed = Message.from_json(json_str)
        
        self.assertEqual(reconstructed.type, original.type)
        self.assertEqual(reconstructed.cmd_id, original.cmd_id)
        self.assertEqual(reconstructed.result, original.result)
        self.assertEqual(reconstructed.exec_time_ms, original.exec_time_ms)


if __name__ == '__main__':
    unittest.main()
