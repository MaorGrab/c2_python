"""
Integration tests for C2 project
Tests end-to-end flows with minimal mocking
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

import unittest
import asyncio
from models.key_manager import KeyManager
from models.encryption_manager import EncryptionManager
from models.message import Message
from models.message_type import MessageType


class TestEncryptionIntegration(unittest.TestCase):
    """Test end-to-end encryption flow between client and server"""
    
    def test_client_server_key_exchange(self):
        """Test complete ECDH key exchange between client and server"""
        # Client side
        client_km = KeyManager()
        client_public_key = client_km.serialized_public_key
        
        # Server side
        server_km = KeyManager()
        server_public_key = server_km.serialized_public_key
        
        # Exchange keys
        client_session_key = client_km.compute_session_key(server_public_key)
        server_session_key = server_km.compute_session_key(client_public_key)
        
        # Both should derive same session key
        self.assertEqual(client_session_key, server_session_key)
        
        # Create encryption managers
        client_em = EncryptionManager(client_session_key)
        server_em = EncryptionManager(server_session_key)
        
        # Test client -> server message
        client_msg = Message.as_register("client-123", "test")
        encrypted = client_em.encrypt(client_msg)
        decrypted = server_em.decrypt(encrypted[4:])  # Remove prefix
        
        self.assertEqual(decrypted.type, client_msg.type)
        self.assertEqual(decrypted.client_id, client_msg.client_id)
        
        # Test server -> client message
        server_msg = Message.as_command("cmd-1", "whoami")
        encrypted = server_em.encrypt(server_msg)
        decrypted = client_em.decrypt(encrypted[4:])  # Remove prefix
        
        self.assertEqual(decrypted.type, server_msg.type)
        self.assertEqual(decrypted.cmd_id, server_msg.cmd_id)
        self.assertEqual(decrypted.command, server_msg.command)
    
    def test_multiple_message_exchange(self):
        """Test multiple messages can be exchanged with same session"""
        # Setup
        client_km = KeyManager()
        server_km = KeyManager()
        
        client_session_key = client_km.compute_session_key(server_km.serialized_public_key)
        server_session_key = server_km.compute_session_key(client_km.serialized_public_key)
        
        client_em = EncryptionManager(client_session_key)
        server_em = EncryptionManager(server_session_key)
        
        # Exchange multiple messages
        messages = [
            Message.as_command("cmd-1", "whoami"),
            Message.as_command("cmd-2", "ls -la"),
            Message.as_command("cmd-3", "pwd"),
        ]
        
        for original in messages:
            encrypted = server_em.encrypt(original)
            decrypted = client_em.decrypt(encrypted[4:])
            
            self.assertEqual(decrypted.type, original.type)
            self.assertEqual(decrypted.cmd_id, original.cmd_id)
            self.assertEqual(decrypted.command, original.command)


class TestMessageProtocolIntegration(unittest.TestCase):
    """Test complete message protocol flow"""
    
    def test_registration_flow(self):
        """Test complete registration message flow"""
        # Client creates registration
        client_id = "client-abc123"
        client_public_key = "client_key_data"
        
        reg_msg = Message.as_register(client_id, client_public_key)
        payload = reg_msg.to_payload(with_prefix=True)
        
        # Server receives and parses
        length = int.from_bytes(payload[:4], 'big')
        msg_data = payload[4:]
        
        self.assertEqual(len(msg_data), length)
        
        received_msg = Message.from_payload(msg_data)
        
        self.assertEqual(received_msg.type, MessageType.REGISTER)
        self.assertEqual(received_msg.client_id, client_id)
        self.assertEqual(received_msg.command, client_public_key)
        
        # Server sends ACK
        server_public_key = "server_key_data"
        ack_msg = Message.as_ack(client_id, server_public_key)
        ack_payload = ack_msg.to_payload(with_prefix=True)
        
        # Client receives ACK
        ack_length = int.from_bytes(ack_payload[:4], 'big')
        ack_data = ack_payload[4:]
        
        received_ack = Message.from_payload(ack_data)
        
        self.assertEqual(received_ack.type, MessageType.ACK)
        self.assertEqual(received_ack.client_id, client_id)
        self.assertEqual(received_ack.command, server_public_key)
    
    def test_command_result_flow(self):
        """Test complete command/result message flow"""
        # Server sends command
        cmd_id = "cmd-xyz789"
        command = "whoami"
        
        cmd_msg = Message.as_command(cmd_id, command)
        cmd_payload = cmd_msg.to_payload(with_prefix=True)
        
        # Client receives command
        cmd_data = cmd_payload[4:]
        received_cmd = Message.from_payload(cmd_data)
        
        self.assertEqual(received_cmd.type, MessageType.COMMAND)
        self.assertEqual(received_cmd.cmd_id, cmd_id)
        self.assertEqual(received_cmd.command, command)
        
        # Client sends result
        result = "root"
        exec_time = 15.5
        
        result_msg = Message.as_result(cmd_id, result, exec_time)
        result_payload = result_msg.to_payload(with_prefix=True)
        
        # Server receives result
        result_data = result_payload[4:]
        received_result = Message.from_payload(result_data)
        
        self.assertEqual(received_result.type, MessageType.RESULT)
        self.assertEqual(received_result.cmd_id, cmd_id)
        self.assertEqual(received_result.result, result)
        self.assertEqual(received_result.exec_time_ms, exec_time)


if __name__ == '__main__':
    unittest.main()
