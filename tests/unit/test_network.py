import pytest
import asyncio
from unittest.mock import AsyncMock

from models.send_receive_msgs import send_message, receive_message

# --- 1. SEND MESSAGE TESTS ---

@pytest.mark.asyncio
async def test_send_message_success(mock_streams):
    """Test that bytes are written and the buffer is drained successfully."""
    payload = b"test message"
    
    _, mock_writer = mock_streams
    result = await send_message(mock_writer, payload)
    
    assert result is True
    mock_writer.write.assert_called_once_with(payload)
    mock_writer.drain.assert_awaited_once()


@pytest.mark.asyncio
async def test_send_message_fails_gracefully_on_write_error(mocker, mock_streams):
    """Test that a synchronous buffer write failure is caught and logged."""
    _, mock_writer = mock_streams
    mock_writer.write.side_effect = Exception("Buffer full")
    mock_logger = mocker.patch('models.send_receive_msgs.logger.error')
    
    result = await send_message(mock_writer, b"data")
    
    assert result is False
    mock_writer.drain.assert_not_awaited()
    mock_logger.assert_called_once_with("Failed to send message: Buffer full")


@pytest.mark.asyncio
async def test_send_message_fails_gracefully_on_drain_error(mocker, mock_streams):
    """Test that an asynchronous network drain failure is caught and logged."""
    _, mock_writer = mock_streams
    mock_writer.drain.side_effect = ConnectionResetError("Peer closed")
    mock_logger = mocker.patch('models.send_receive_msgs.logger.error')
    
    result = await send_message(mock_writer, b"data")
    
    assert result is False
    mock_writer.write.assert_called_once()
    mock_logger.assert_called_once_with("Failed to send message: Peer closed")


# --- 2. RECEIVE MESSAGE TESTS ---

@pytest.mark.asyncio
async def test_receive_message_success(mock_streams):
    """Test the two-step read process: 4-byte prefix, then the exact payload length."""
    payload = b"hello c2"
    length_prefix = len(payload).to_bytes(4, 'big')
    
    mock_reader, _ = mock_streams
    # 1st call to readexactly gets the prefix, 2nd call gets the payload
    mock_reader.readexactly.side_effect = [length_prefix, payload]
    
    result = await receive_message(mock_reader)
    
    assert result == payload
    assert mock_reader.readexactly.call_count == 2
    mock_reader.readexactly.assert_any_call(4)
    mock_reader.readexactly.assert_any_call(len(payload))


@pytest.mark.asyncio
async def test_receive_message_bubbles_up_initial_incomplete_read(mock_streams):
    """Test that silent TCP drops during the prefix read are bubbled up."""
    mock_reader, _ = mock_streams
    mock_reader.readexactly.side_effect = asyncio.IncompleteReadError(b'', 4)
    
    with pytest.raises(asyncio.IncompleteReadError):
        await receive_message(mock_reader)


@pytest.mark.asyncio
async def test_receive_message_bubbles_up_mid_payload_drop(mock_streams):
    """Test connection drop after length prefix is read but before payload finishes."""
    # Prefix says 100 bytes are coming, but the connection dies after 10 bytes
    mock_reader, _ = mock_streams
    mock_reader.readexactly.side_effect = [
        (100).to_bytes(4, 'big'), 
        asyncio.IncompleteReadError(b'1234567890', 100)
    ]
    
    with pytest.raises(asyncio.IncompleteReadError):
        await receive_message(mock_reader)
        
    # Prove the state machine made it to the second read attempt
    assert mock_reader.readexactly.call_count == 2


@pytest.mark.asyncio
async def test_receive_message_bubbles_up_connection_reset(mocker, mock_streams):
    """Test that aggressive TCP drops (RST packets) are logged and bubbled up."""
    mock_reader, _ = mock_streams
    mock_reader.readexactly.side_effect = ConnectionResetError("RST")
    mock_logger = mocker.patch('models.send_receive_msgs.logger.info')
    
    with pytest.raises(ConnectionResetError):
        await receive_message(mock_reader)
        
    mock_logger.assert_called_once_with("Connection reset by peer")


@pytest.mark.asyncio
async def test_receive_message_absorbs_generic_exceptions(mocker, mock_streams):
    """Test that unknown errors safely return None instead of crashing the listener loop."""
    mock_reader, _ = mock_streams
    mock_reader.readexactly.side_effect = Exception("Corrupt memory")
    mock_logger = mocker.patch('models.send_receive_msgs.logger.error')
    
    result = await receive_message(mock_reader)
    
    assert result is None
    mock_logger.assert_called_once_with("Failed to receive message: Corrupt memory")


@pytest.mark.asyncio
async def test_receive_message_catches_dos_memory_errors(mocker, mock_streams):
    """Test that massive payload length attacks (DoS) don't crash the server."""
    # Attacker sends max 32-bit unsigned integer prefix: ~4.2 Gigabytes
    malicious_prefix = (4294967295).to_bytes(4, 'big')
    
    mock_reader, _ = mock_streams
    # Python asyncio streams raise LimitOverrunError or MemoryError on massive allocations
    mock_reader.readexactly.side_effect = [
        malicious_prefix, 
        MemoryError("Out of memory")
    ]
    mock_logger = mocker.patch('models.send_receive_msgs.logger.error')
    
    result = await receive_message(mock_reader)
    
    # Assert the server survived and absorbed the memory allocation failure
    assert result is None
    mock_logger.assert_called_once_with("Failed to receive message: Out of memory")