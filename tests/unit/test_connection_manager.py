"""
Unit tests for ConnectionManager
Tests connection management and reconnection logic
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from models.connection_manager import ConnectionManager


@pytest.fixture
def cm():
    """Provide ConnectionManager instance"""
    return ConnectionManager("127.0.0.1", 5000, "test-client")


@pytest.mark.asyncio
@patch('models.connection_manager.asyncio.open_connection')
@patch('models.connection_manager.validate_server_certificate')
@patch('models.connection_manager.TLSSessionHelper')
async def test_open_connection_success(mock_tls, mock_validate, mock_open, cm):
    """Test successful connection establishment"""
    # Mock TLS context
    mock_tls_instance = Mock()
    mock_tls_instance.create_context.return_value = Mock()
    mock_tls.return_value = mock_tls_instance
    
    # Mock connection
    mock_reader = AsyncMock()
    mock_writer = Mock()
    mock_open.return_value = (mock_reader, mock_writer)
    mock_validate.return_value = True
    
    result = await cm._open_connection()
    
    assert result is True
    assert cm.reader == mock_reader
    assert cm.writer == mock_writer
    mock_open.assert_called_once()


@pytest.mark.asyncio
@patch('models.connection_manager.asyncio.open_connection')
@patch('models.connection_manager.TLSSessionHelper')
async def test_open_connection_refused(mock_tls, mock_open, cm):
    """Test connection refused handling"""
    mock_tls_instance = Mock()
    mock_tls_instance.create_context.return_value = Mock()
    mock_tls.return_value = mock_tls_instance
    
    mock_open.side_effect = ConnectionRefusedError("Connection refused")
    
    result = await cm._open_connection()
    
    assert result is False
    assert cm.reader is None
    assert cm.writer is None


@pytest.mark.asyncio
@patch('models.connection_manager.asyncio.open_connection')
@patch('models.connection_manager.validate_server_certificate')
@patch('models.connection_manager.TLSSessionHelper')
async def test_open_connection_certificate_validation_fails(mock_tls, mock_validate, mock_open, cm):
    """Test connection fails when certificate validation fails"""
    mock_tls_instance = Mock()
    mock_tls_instance.create_context.return_value = Mock()
    mock_tls.return_value = mock_tls_instance
    
    mock_reader = AsyncMock()
    mock_writer = Mock()
    mock_open.return_value = (mock_reader, mock_writer)
    mock_validate.return_value = False
    
    result = await cm._open_connection()
    
    assert result is False


@pytest.mark.asyncio
async def test_reconnection_loop_retries(cm):
    """Test reconnection loop retries on failure"""
    with patch.object(cm, '_open_connection') as mock_open:
        # Multiple failures to trigger retries
        mock_open.side_effect = [False, False, False, True]
        
        # Start reconnection loop
        cm._set_events_reconnect()
        reconnect_task = asyncio.create_task(cm._reconnection_loop())
        
        # Wait for reconnection attempts
        await asyncio.sleep(1.2)
        
        # Cancel task
        reconnect_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await reconnect_task
        
        # Verify multiple connection attempts
        assert mock_open.call_count >= 2


@pytest.mark.asyncio
async def test_trigger_reconnection_clears_streams(cm):
    """Test trigger_reconnection clears reader/writer"""
    # Setup mock writer
    mock_writer = Mock()
    mock_writer.is_closing.return_value = False
    mock_writer.close = Mock()
    mock_writer.wait_closed = AsyncMock()
    
    cm.reader = AsyncMock()
    cm.writer = mock_writer
    cm._reconnection_task = asyncio.create_task(asyncio.sleep(10))
    
    await cm.trigger_reconnection()
    
    assert cm.reader is None
    assert cm.writer is None
    mock_writer.close.assert_called_once()


@pytest.mark.asyncio
async def test_wait_connected_blocks_until_connected(cm):
    """Test wait_connected blocks until connection established"""
    with patch.object(cm, '_open_connection', return_value=True):
        # Start connection manager
        await cm.start()
        
        # wait_connected should return immediately since connected
        result = await asyncio.wait_for(cm.wait_connected(), timeout=1.0)
        
        assert result is True
        
        # Cleanup
        await cm.shutdown()


@pytest.mark.asyncio
async def test_shutdown_cancels_reconnection_task(cm):
    """Test shutdown cancels reconnection task"""
    # Create mock reconnection task
    cm._reconnection_task = asyncio.create_task(asyncio.sleep(10))
    
    await cm.shutdown()
    
    assert cm._reconnection_task is None


@pytest.mark.asyncio
async def test_reset_streams_handles_already_closed(cm):
    """Test _reset_streams handles already-closed writer gracefully"""
    mock_writer = Mock()
    mock_writer.is_closing.return_value = True  # Already closed
    
    cm.reader = AsyncMock()
    cm.writer = mock_writer
    
    # Should not raise exception
    await cm._reset_streams()
    
    assert cm.reader is None
    assert cm.writer is None


@pytest.mark.asyncio
async def test_wait_connected_returns_false_when_inactive(cm):
    """Test wait_connected returns False when not active"""
    # Don't start connection manager (not active)
    result = await cm.wait_connected()
    
    assert result is False
