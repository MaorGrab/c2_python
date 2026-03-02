"""
Unit tests for ConnectionManager
Tests connection management and reconnection logic
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from models.connection_manager import ConnectionManager


@pytest.mark.asyncio
@patch('models.connection_manager.asyncio.open_connection')
@patch('models.connection_manager.validate_server_certificate')
@patch('models.connection_manager.TLSSessionHelper')
async def test_open_connection_success(mock_tls, mock_validate, mock_open, connection_manager):
    """Test successful connection establishment"""
    mock_tls_instance = Mock()
    mock_tls_instance.create_context.return_value = Mock()
    mock_tls.return_value = mock_tls_instance
    
    mock_reader = AsyncMock()
    mock_writer = Mock()
    mock_open.return_value = (mock_reader, mock_writer)
    mock_validate.return_value = True
    
    result = await connection_manager._open_connection()
    
    assert result is True
    assert connection_manager.reader == mock_reader
    assert connection_manager.writer == mock_writer
    mock_open.assert_called_once()


@pytest.mark.asyncio
@patch('models.connection_manager.asyncio.open_connection')
@patch('models.connection_manager.TLSSessionHelper')
async def test_open_connection_refused(mock_tls, mock_open, connection_manager):
    """Test connection refused handling"""
    mock_tls_instance = Mock()
    mock_tls_instance.create_context.return_value = Mock()
    mock_tls.return_value = mock_tls_instance
    
    mock_open.side_effect = ConnectionRefusedError("Connection refused")
    
    result = await connection_manager._open_connection()
    
    assert result is False
    assert connection_manager.reader is None
    assert connection_manager.writer is None


@pytest.mark.asyncio
@patch('models.connection_manager.asyncio.open_connection')
@patch('models.connection_manager.validate_server_certificate')
@patch('models.connection_manager.TLSSessionHelper')
async def test_open_connection_certificate_validation_fails(mock_tls, mock_validate, mock_open, connection_manager):
    """Test connection fails when certificate validation fails"""
    mock_tls_instance = Mock()
    mock_tls_instance.create_context.return_value = Mock()
    mock_tls.return_value = mock_tls_instance
    
    mock_reader = AsyncMock()
    mock_writer = Mock()
    mock_open.return_value = (mock_reader, mock_writer)
    mock_validate.return_value = False
    
    result = await connection_manager._open_connection()
    
    assert result is False


@pytest.mark.asyncio
async def test_reconnection_loop_retries(connection_manager):
    """Test reconnection loop retries on failure"""
    with patch.object(connection_manager, '_open_connection') as mock_open:
        mock_open.side_effect = [False, False, True]
        
        connection_manager._set_events_reconnect()
        reconnect_task = asyncio.create_task(connection_manager._reconnection_loop())
        
        # Wait longer for retries with delay
        await asyncio.sleep(1.5)
        
        reconnect_task.cancel()
        try:
            await reconnect_task
        except asyncio.CancelledError:
            pass
        
        assert mock_open.call_count >= 2


@pytest.mark.asyncio
async def test_trigger_reconnection_clears_streams(connection_manager):
    """Test trigger_reconnection clears reader/writer and cancels task"""
    mock_writer = Mock()
    mock_writer.is_closing.return_value = False
    mock_writer.close = Mock()
    mock_writer.wait_closed = AsyncMock()
    
    connection_manager.reader = AsyncMock()
    connection_manager.writer = mock_writer
    reconnect_task = asyncio.create_task(asyncio.sleep(10))
    connection_manager._reconnection_task = reconnect_task
    
    await connection_manager.trigger_reconnection()
    
    # Wait for task cancellation to propagate
    try:
        await asyncio.wait_for(reconnect_task, timeout=0.1)
    except (asyncio.CancelledError, asyncio.TimeoutError):
        pass
    
    assert connection_manager.reader is None
    assert connection_manager.writer is None
    mock_writer.close.assert_called_once()
    assert reconnect_task.done()


@pytest.mark.asyncio
async def test_wait_connected_blocks_until_connected(connection_manager):
    """Test wait_connected blocks until connection established"""
    with patch.object(connection_manager, '_open_connection', return_value=True):
        await connection_manager.start()
        
        result = await asyncio.wait_for(connection_manager.wait_connected(), timeout=1.0)
        
        assert result is True
        
        await connection_manager.shutdown()


@pytest.mark.asyncio
async def test_shutdown_cancels_reconnection_task(connection_manager):
    """Test shutdown cancels reconnection task"""
    connection_manager._reconnection_task = asyncio.create_task(asyncio.sleep(10))
    
    await connection_manager.shutdown()
    
    assert connection_manager._reconnection_task is None


@pytest.mark.asyncio
async def test_reset_streams_handles_already_closed(connection_manager):
    """Test _reset_streams handles already-closed writer gracefully"""
    mock_writer = Mock()
    mock_writer.is_closing.return_value = True
    
    connection_manager.reader = AsyncMock()
    connection_manager.writer = mock_writer
    
    await connection_manager._reset_streams()
    
    assert connection_manager.reader is None
    assert connection_manager.writer is None


@pytest.mark.asyncio
async def test_wait_connected_returns_false_when_inactive(connection_manager):
    """Test wait_connected returns False when not active"""
    result = await connection_manager.wait_connected()
    
    assert result is False
