"""
Unit tests for network communication
Tests send_message and receive_message functions
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

import unittest
import asyncio
from unittest.mock import AsyncMock, Mock, patch
from models.send_receive_msgs import send_message, receive_message


class TestNetworkCommunication(unittest.IsolatedAsyncioTestCase):
    """Test network message sending and receiving"""
    
    async def test_send_message_success(self):
        """Test successful message sending"""
        mock_writer = Mock()
        mock_writer.write = Mock()
        mock_writer.drain = AsyncMock()
        
        message = b"test message"
        result = await send_message(mock_writer, message)
        
        self.assertTrue(result)
        mock_writer.write.assert_called_once_with(message)
        mock_writer.drain.assert_called_once()
    
    async def test_send_message_failure(self):
        """Test message sending with exception"""
        mock_writer = Mock()
        mock_writer.write = Mock(side_effect=Exception("Connection error"))
        mock_writer.drain = AsyncMock()
        
        message = b"test message"
        result = await send_message(mock_writer, message)
        
        self.assertFalse(result)
    
    async def test_receive_message_success(self):
        """Test successful message receiving"""
        mock_reader = AsyncMock()
        
        # Mock length prefix (4 bytes) and payload
        payload = b"test payload"
        length = len(payload)
        length_bytes = length.to_bytes(4, 'big')
        
        mock_reader.readexactly = AsyncMock(side_effect=[length_bytes, payload])
        
        result = await receive_message(mock_reader)
        
        self.assertEqual(result, payload)
        self.assertEqual(mock_reader.readexactly.call_count, 2)
    
    async def test_receive_message_incomplete_read(self):
        """Test receiving message with incomplete read"""
        mock_reader = AsyncMock()
        mock_reader.readexactly = AsyncMock(side_effect=asyncio.IncompleteReadError(b'', 4))
        
        with self.assertRaises(asyncio.IncompleteReadError):
            await receive_message(mock_reader)
    
    async def test_receive_message_connection_reset(self):
        """Test receiving message with connection reset"""
        mock_reader = AsyncMock()
        mock_reader.readexactly = AsyncMock(side_effect=ConnectionResetError("Connection reset"))
        
        with self.assertRaises(ConnectionResetError):
            await receive_message(mock_reader)
    
    async def test_receive_message_general_exception(self):
        """Test receiving message with general exception"""
        mock_reader = AsyncMock()
        mock_reader.readexactly = AsyncMock(side_effect=Exception("Unknown error"))
        
        result = await receive_message(mock_reader)
        
        self.assertIsNone(result)
    
    async def test_send_receive_roundtrip(self):
        """Test complete send/receive roundtrip"""
        # Create mock writer
        mock_writer = Mock()
        sent_data = []
        
        def capture_write(data):
            sent_data.append(data)
        
        mock_writer.write = Mock(side_effect=capture_write)
        mock_writer.drain = AsyncMock()
        
        # Send message with length prefix
        payload = b"test message"
        length_prefix = len(payload).to_bytes(4, 'big')
        full_message = length_prefix + payload
        
        result = await send_message(mock_writer, full_message)
        self.assertTrue(result)
        
        # Create mock reader with sent data
        mock_reader = AsyncMock()
        mock_reader.readexactly = AsyncMock(side_effect=[length_prefix, payload])
        
        # Receive message
        received = await receive_message(mock_reader)
        
        self.assertEqual(received, payload)


if __name__ == '__main__':
    unittest.main()
