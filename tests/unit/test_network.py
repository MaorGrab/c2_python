"""
Unit tests for network communication
Tests send_message and receive_message functions
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, Mock
from models.send_receive_msgs import send_message, receive_message


@pytest.mark.asyncio
async def test_send_message_success():
    """Test successful message sending"""
    mock_writer = Mock()
    mock_writer.write = Mock()
    mock_writer.drain = AsyncMock()
    
    message = b"test message"
    result = await send_message(mock_writer, message)
    
    assert result is True
    mock_writer.write.assert_called_once_with(message)
    mock_writer.drain.assert_called_once()


@pytest.mark.asyncio
async def test_send_message_failure():
    """Test message sending with exception"""
    mock_writer = Mock()
    mock_writer.write = Mock(side_effect=Exception("Connection error"))
    mock_writer.drain = AsyncMock()
    
    message = b"test message"
    result = await send_message(mock_writer, message)
    
    assert result is False


@pytest.mark.asyncio
async def test_receive_message_success():
    """Test successful message receiving"""
    mock_reader = AsyncMock()
    
    payload = b"test payload"
    length = len(payload)
    length_bytes = length.to_bytes(4, 'big')
    
    mock_reader.readexactly = AsyncMock(side_effect=[length_bytes, payload])
    
    result = await receive_message(mock_reader)
    
    assert result == payload
    assert mock_reader.readexactly.call_count == 2


@pytest.mark.asyncio
async def test_receive_message_incomplete_read():
    """Test receiving message with incomplete read"""
    mock_reader = AsyncMock()
    mock_reader.readexactly = AsyncMock(side_effect=asyncio.IncompleteReadError(b'', 4))
    
    with pytest.raises(asyncio.IncompleteReadError):
        await receive_message(mock_reader)


@pytest.mark.asyncio
async def test_receive_message_connection_reset():
    """Test receiving message with connection reset"""
    mock_reader = AsyncMock()
    mock_reader.readexactly = AsyncMock(side_effect=ConnectionResetError("Connection reset"))
    
    with pytest.raises(ConnectionResetError):
        await receive_message(mock_reader)


@pytest.mark.asyncio
async def test_receive_message_general_exception():
    """Test receiving message with general exception"""
    mock_reader = AsyncMock()
    mock_reader.readexactly = AsyncMock(side_effect=Exception("Unknown error"))
    
    result = await receive_message(mock_reader)
    
    assert result is None


@pytest.mark.asyncio
async def test_send_receive_roundtrip():
    """Test complete send/receive roundtrip"""
    mock_writer = Mock()
    sent_data = []
    
    def capture_write(data):
        sent_data.append(data)
    
    mock_writer.write = Mock(side_effect=capture_write)
    mock_writer.drain = AsyncMock()
    
    payload = b"test message"
    length_prefix = len(payload).to_bytes(4, 'big')
    full_message = length_prefix + payload
    
    result = await send_message(mock_writer, full_message)
    assert result is True
    
    mock_reader = AsyncMock()
    mock_reader.readexactly = AsyncMock(side_effect=[length_prefix, payload])
    
    received = await receive_message(mock_reader)
    
    assert received == payload
