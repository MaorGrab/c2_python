"""
Unit tests for ClientState
Tests client lifecycle management with mocked network components
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

import unittest
import asyncio
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from models.client_state import ClientState
from models.connection_status import ConnectionStatus
from models.message import Message
from models.message_type import MessageType
from models.command_type import CommandType


class TestClientState(unittest.TestCase):
    """Test ClientState lifecycle management"""
    
    def setUp(self):
        """Setup mock reader/writer for each test"""
        self.mock_reader = Mock()  # Changed from AsyncMock
        self.mock_reader.feed_eof = Mock()  # Explicitly mock as sync
        self.mock_writer = Mock()
        self.mock_writer.is_closing = Mock(return_value=False)
        self.mock_writer.close = Mock()
        self.mock_writer.wait_closed = AsyncMock()
        
        self.client_state = ClientState("test-client", self.mock_reader, self.mock_writer)
    
    def test_initialization(self):
        """Test ClientState initialization"""
        self.assertEqual(self.client_state.client_id, "test-client")
        self.assertEqual(self.client_state.status, ConnectionStatus.CONNECTED)
        self.assertIsNotNone(self.client_state.command_queue)
        self.assertIsNotNone(self.client_state._encryption_manager)
    
    def test_setup_encryption(self):
        """Test encryption setup with peer public key"""
        # Create another encryption manager to get peer key
        from models.encryption_manager import EncryptionManager
        peer_em = EncryptionManager()
        peer_public_key = peer_em.public_key_b64
        
        # Setup encryption
        server_public_key = self.client_state.setup_encryption(peer_public_key)
        
        self.assertIsNotNone(server_public_key)
        self.assertIsNotNone(self.client_state._encryption_manager.session_key)
    
    def test_add_command(self):
        """Test adding command to queue"""
        cmd_id = self.client_state.add_command("whoami")
        
        self.assertIsNotNone(cmd_id)
        self.assertFalse(self.client_state.command_queue.empty())
    
    def test_kill_command(self):
        """Test kill command sets killed status"""
        result = self.client_state.kill()
        
        self.assertTrue(result)
        self.assertEqual(self.client_state.status, ConnectionStatus.KILLED)
        self.assertFalse(self.client_state.command_queue.empty())
    
    def test_status_transitions(self):
        """Test status transition methods"""
        self.client_state.set_killed()
        self.assertEqual(self.client_state.status, ConnectionStatus.KILLED)
        self.assertTrue(self.client_state.is_killed)
        
        self.client_state.set_disconnected()
        self.assertEqual(self.client_state.status, ConnectionStatus.DISCONNECTED)
        self.assertTrue(self.client_state.is_disconnected)
    
    def test_is_connected_property(self):
        """Test is_connected property"""
        self.assertTrue(self.client_state.is_connected)
        
        self.client_state.set_killed()
        self.assertFalse(self.client_state.is_connected)
    
    def test_encrypt_message_without_setup_raises_error(self):
        """Test that encrypting without setup raises error"""
        msg = Message.as_command("cmd-1", "test")
        
        with self.assertRaises(RuntimeError):
            self.client_state.encrypt_message(msg)
    
    def test_decrypt_message_without_setup_raises_error(self):
        """Test that decrypting without setup raises error"""
        with self.assertRaises(RuntimeError):
            self.client_state.decrypt_message(b"encrypted_data")
    
    def test_encrypt_decrypt_with_setup(self):
        """Test encryption/decryption after setup"""
        # Setup encryption with peer
        from models.encryption_manager import EncryptionManager
        peer_em = EncryptionManager()
        peer_key = peer_em.public_key_b64
        server_key = self.client_state.setup_encryption(peer_key)
        peer_em.establish_session_key(server_key)
        
        # Test encryption/decryption
        original = Message.as_command("cmd-123", "whoami")
        encrypted = self.client_state.encrypt_message(original)
        
        self.assertIsInstance(encrypted, bytes)
        
        # Decrypt (remove length prefix)
        decrypted = peer_em.decrypt(encrypted[4:])
        
        self.assertEqual(decrypted.type, original.type)
        self.assertEqual(decrypted.cmd_id, original.cmd_id)


class TestClientStateAsync(unittest.IsolatedAsyncioTestCase):
    """Test ClientState async methods"""
    
    async def asyncSetUp(self):
        """Setup mock reader/writer for async tests"""
        self.mock_reader = Mock()  # Changed from AsyncMock
        self.mock_reader.feed_eof = Mock()  # Explicitly mock as sync
        self.mock_writer = Mock()
        self.mock_writer.is_closing = Mock(return_value=False)
        self.mock_writer.close = Mock()
        self.mock_writer.wait_closed = AsyncMock()
        
        self.client_state = ClientState("test-client", self.mock_reader, self.mock_writer)
        
        # Setup encryption for tests
        from models.encryption_manager import EncryptionManager
        peer_em = EncryptionManager()
        peer_key = peer_em.public_key_b64
        self.client_state.setup_encryption(peer_key)
    
    @patch('models.client_state.receive_message')
    @patch('models.client_state.send_message')
    async def test_execute_command(self, mock_send, mock_receive):
        """Test command execution"""
        mock_send.return_value = True
        
        cmd_data = {"cmd_id": "cmd-1", "command": "whoami"}
        await self.client_state._execute_command(cmd_data)
        
        self.assertIn("cmd-1", self.client_state.pending_results)
        mock_send.assert_called_once()
    
    async def test_cleanup_connection(self):
        """Test connection cleanup"""
        await self.client_state._cleanup_connection()
        
        self.mock_writer.close.assert_called_once()
        self.mock_writer.wait_closed.assert_called_once()
        self.assertIsNone(self.client_state.reader)
        self.assertIsNone(self.client_state.writer)
    
    async def test_cleanup_queues(self):
        """Test queue cleanup"""
        self.client_state.add_command("test1")
        self.client_state.add_command("test2")
        self.client_state.pending_results["cmd-1"] = "test"
        
        self.client_state._cleanup_queue()
        
        self.assertEqual(len(self.client_state.pending_results), 1)


if __name__ == '__main__':
    unittest.main()
