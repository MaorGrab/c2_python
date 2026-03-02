"""
Unit tests for ClientManager
Tests client registry and coordination functionality
"""

import unittest
import asyncio
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from models.client_manager import ClientManager
from models.client_state import ClientState
from models.message import Message
from models.message_type import MessageType


class TestClientManager(unittest.TestCase):
    """Test ClientManager registry functionality"""
    
    def setUp(self):
        """Setup ClientManager for each test"""
        self.manager = ClientManager()
    
    def test_initialization(self):
        """Test ClientManager initialization"""
        self.assertEqual(len(self.manager.clients), 0)
    
    def test_set_shutdown_event(self):
        """Test ClientManager has no shutdown event (removed in refactor)"""
        # Shutdown event removed in refactor - test passes
        self.assertFalse(hasattr(self.manager, '_shutdown_event'))
    
    def test_get_client_not_found(self):
        """Test getting non-existent client returns None"""
        client = self.manager.get_client("non-existent")
        
        self.assertIsNone(client)
    
    def test_get_client_found(self):
        """Test getting existing client"""
        mock_reader = Mock()  # Changed from AsyncMock
        mock_writer = Mock()
        client_state = ClientState("test-client", mock_reader, mock_writer)
        
        self.manager.clients["test-client"] = client_state
        
        result = self.manager.get_client("test-client")
        
        self.assertEqual(result, client_state)
    
    def test_list_clients_empty(self):
        """Test listing clients when empty"""
        clients = self.manager.list_clients()
        
        self.assertEqual(len(clients), 0)
    
    def test_list_clients_returns_copy(self):
        """Test that list_clients returns a copy"""
        mock_reader = Mock()  # Changed from AsyncMock
        mock_writer = Mock()
        client_state = ClientState("test-client", mock_reader, mock_writer)
        
        self.manager.clients["test-client"] = client_state
        
        clients = self.manager.list_clients()
        clients["new-client"] = None  # Modify copy
        
        # Original should be unchanged
        self.assertNotIn("new-client", self.manager.clients)
    
    def test_add_command_to_queue_client_not_found(self):
        """Test adding command to non-existent client"""
        result = self.manager.add_command_to_queue("non-existent", "whoami")
        
        self.assertFalse(result)
    
    def test_add_command_to_queue_success(self):
        """Test adding command to existing client"""
        mock_reader = Mock()  # Changed from AsyncMock
        mock_writer = Mock()
        client_state = ClientState("test-client", mock_reader, mock_writer)
        
        self.manager.clients["test-client"] = client_state
        
        result = self.manager.add_command_to_queue("test-client", "whoami")
        
        self.assertTrue(result)
        self.assertFalse(client_state.command_queue.empty())
    
    def test_kill_client_not_found(self):
        """Test killing non-existent client"""
        result = self.manager.kill_client("non-existent")
        
        self.assertFalse(result)
    
    def test_kill_client_success(self):
        """Test killing existing client"""
        mock_reader = Mock()  # Changed from AsyncMock
        mock_writer = Mock()
        client_state = ClientState("test-client", mock_reader, mock_writer)
        
        self.manager.clients["test-client"] = client_state
        
        result = self.manager.kill_client("test-client")
        
        self.assertTrue(result)
        self.assertTrue(client_state.is_killed)


class TestClientManagerAsync(unittest.IsolatedAsyncioTestCase):
    """Test ClientManager async methods"""
    
    async def asyncSetUp(self):
        """Setup ClientManager for async tests"""
        self.manager = ClientManager()
    
    @patch('models.client_manager.receive_message')
    @patch('models.client_manager.send_message')
    async def test_register_client_success(self, mock_send, mock_receive):
        """Test successful client registration"""
        mock_reader = AsyncMock()
        mock_writer = Mock()
        mock_writer.close = Mock()
        
        # Create valid public key
        from models.encryption_manager import EncryptionManager
        em = EncryptionManager()
        valid_public_key = em.public_key_b64
        
        # Mock registration message with valid key
        reg_msg = Message.as_register("client-123", valid_public_key)
        mock_receive.return_value = reg_msg.to_payload(with_prefix=False)
        mock_send.return_value = True
        
        client_state = await self.manager.register_client(mock_reader, mock_writer)
        
        self.assertIsNotNone(client_state)
        self.assertEqual(client_state.client_id, "client-123")
        self.assertIn("client-123", self.manager.clients)
        mock_send.assert_called_once()
    
    @patch('models.client_manager.receive_message')
    async def test_register_client_invalid_message(self, mock_receive):
        """Test registration with invalid message type"""
        mock_reader = AsyncMock()
        mock_writer = Mock()
        mock_writer.close = Mock()
        
        # Mock invalid message
        invalid_msg = Message.as_command("cmd-1", "test")
        mock_receive.return_value = invalid_msg.to_payload(with_prefix=False)
        
        client_state = await self.manager.register_client(mock_reader, mock_writer)
        
        self.assertIsNone(client_state)
        mock_writer.close.assert_called_once()
    
    @patch('models.client_manager.receive_message')
    async def test_register_client_exception(self, mock_receive):
        """Test registration with exception"""
        mock_reader = AsyncMock()
        mock_writer = Mock()
        
        mock_receive.side_effect = Exception("Connection error")
        
        client_state = await self.manager.register_client(mock_reader, mock_writer)
        
        self.assertIsNone(client_state)
    
    async def test_handle_client_loop_delegates_to_client(self):
        """Test that handle_client_loop delegates to client's run_lifecycle"""
        mock_reader = AsyncMock()
        mock_writer = Mock()
        client_state = ClientState("test-client", mock_reader, mock_writer)
        
        # Mock run_lifecycle
        client_state.run_lifecycle = AsyncMock()
        
        await self.manager.handle_client_loop(client_state)
        
        client_state.run_lifecycle.assert_called_once()
    
    async def test_close_all_clients(self):
        """Test closing all client connections"""
        mock_reader1 = Mock()  # Changed from AsyncMock
        mock_reader1.feed_eof = Mock()
        mock_writer1 = Mock()
        mock_writer1.is_closing = Mock(return_value=False)
        mock_writer1.close = Mock()
        mock_writer1.wait_closed = AsyncMock()
        
        client1 = ClientState("client-1", mock_reader1, mock_writer1)
        
        self.manager.clients["client-1"] = client1
        
        await self.manager.close_all_clients()
        
        self.assertEqual(len(self.manager.clients), 0)


if __name__ == '__main__':
    unittest.main()
