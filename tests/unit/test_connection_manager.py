import pytest
import asyncio

@pytest.mark.asyncio
async def test_open_connection_refused(mocker, connection_manager):
    # Arrange: Mock the SSL context to avoid unrelated errors
    mocker.patch('models.connection_manager.TLSSessionHelper')
    
    # Arrange: Force the connection to fail
    mocker.patch(
        'models.connection_manager.asyncio.open_connection', 
        side_effect=ConnectionRefusedError("Connection refused")
    )
    
    # Act
    result = await connection_manager._open_connection()
    
    # Assert: Must handle the error gracefully and return False
    assert result is False

@pytest.mark.asyncio
async def test_reset_streams_cleans_up_resources(mocker, connection_manager):
    # Arrange: Create a mock writer (wait_closed is an async method)
    mock_writer = mocker.AsyncMock()
    mock_writer.is_closing = mocker.Mock(return_value=False)
    mock_writer.close = mocker.Mock(return_value=None)
    connection_manager.writer = mock_writer
    connection_manager.reader = mocker.Mock()
    
    # Act
    await connection_manager._reset_streams()
    
    # Assert: Verify the socket was closed
    mock_writer.close.assert_called_once()
    mock_writer.wait_closed.assert_awaited_once()
    
    # Assert: Verify pointers are cleared to allow garbage collection
    assert connection_manager.writer is None
    assert connection_manager.reader is None

@pytest.mark.asyncio
async def test_reset_streams_clears_pointers_on_exception(mocker, connection_manager):
    """Test that streams are cleared even if closing the writer raises an error."""
    # Arrange: Mock a writer that throws an error when wait_closed is awaited
    mock_writer = mocker.AsyncMock()
    mock_writer.is_closing = mocker.Mock(return_value=False)
    mock_writer.close = mocker.Mock()  # Force close to be sync so it doesn't return an un-awaited coroutine
    mock_writer.wait_closed.side_effect = Exception("OS Socket Error")
    
    connection_manager.reader = mocker.Mock()
    connection_manager.writer = mock_writer
    
    # Act
    await connection_manager._reset_streams()
    
    # Assert: The pointers MUST be None, proving the `finally` block executed
    assert connection_manager.reader is None
    assert connection_manager.writer is None

@pytest.mark.asyncio
async def test_reconnection_loop_recovers(mocker, connection_manager):
    # Arrange: Simulate first attempt failing, second succeeding
    mock_open = mocker.patch.object(
        connection_manager, '_open_connection', side_effect=[False, True]
    )
    mock_sleep = mocker.patch('models.connection_manager.asyncio.sleep', new_callable=mocker.AsyncMock)
    mock_reset = mocker.patch.object(connection_manager, '_reset_streams')
    
    # Act: Start the loop as a task so it doesn't block
    connection_manager._set_events_reconnect()
    loop_task = asyncio.create_task(connection_manager._reconnection_loop())
    
    # Wait for the successful connection event to be set (with a timeout to prevent hanging tests)
    await asyncio.wait_for(connection_manager._connected.wait(), timeout=1.0)
    
    # Cleanup: Cancel the loop so the test can finish
    loop_task.cancel()
    
    # Assert: It should have tried to open twice, and slept once in between
    assert mock_open.call_count == 2
    mock_sleep.assert_awaited_once_with(1)

@pytest.mark.asyncio
async def test_shutdown_cancels_active_task(mocker, connection_manager):
    """Test that an active task is properly cancelled and state is cleared."""
    # Arrange: Mock the external cancel_task helper (it's an async function)
    mock_cancel = mocker.patch(
        'models.connection_manager.cancel_task', 
        new_callable=mocker.AsyncMock
    )
    
    # Arrange: Simulate an active background task using a dummy mock
    dummy_task = mocker.Mock()
    connection_manager._reconnection_task = dummy_task
    
    # Pre-assertion sanity check
    assert connection_manager.is_active is True
    
    # Act
    await connection_manager.shutdown()
    
    # Assert: Verify the external helper was awaited with our specific task
    mock_cancel.assert_awaited_once_with(dummy_task)
    
    # Assert: Verify the internal state was cleanly reset
    assert connection_manager._reconnection_task is None
    assert connection_manager.is_active is False


@pytest.mark.asyncio
async def test_shutdown_inactive_is_noop(mocker, connection_manager):
    """Test that calling shutdown when already inactive does nothing."""
    # Arrange: Mock the external helper
    mock_cancel = mocker.patch(
        'models.connection_manager.cancel_task', 
        new_callable=mocker.AsyncMock
    )
    
    # Arrange: Ensure the manager is inactive (default state from our fixture)
    assert connection_manager.is_active is False
    
    # Act
    await connection_manager.shutdown()
    
    # Assert: The cancel function should NEVER be called
    mock_cancel.assert_not_called()
    assert connection_manager._reconnection_task is None

@pytest.mark.asyncio
async def test_trigger_reconnection_inactive_is_noop(mocker, connection_manager):
    """Test trigger_reconnection returns early if manager is not active."""
    # Arrange
    mock_reset = mocker.patch.object(connection_manager, '_reset_streams')
    assert connection_manager.is_active is False
    
    # Act
    await connection_manager.trigger_reconnection()
    
    # Assert
    mock_reset.assert_not_called()
    assert connection_manager._reconnect.is_set() is False

@pytest.mark.asyncio
async def test_reconnection_loop_cleans_up_on_cancel(mocker, connection_manager):
    """Test that cancelling the loop triggers stream cleanup."""
    
    # 1. ISOLATE: Mock the internal methods the loop relies on
    mock_reset = mocker.patch.object(connection_manager, '_reset_streams', new_callable=mocker.AsyncMock)
    mock_open = mocker.patch.object(connection_manager, '_open_connection', new_callable=mocker.AsyncMock)
    
    # Make open_connection hang indefinitely so the task is predictably waiting there
    # This prevents the loop from spinning wildly and hitting asyncio.sleep
    async def hang_forever():
        await asyncio.Future() 
    mock_open.side_effect = hang_forever
    
    # 2. Start the loop
    connection_manager._set_events_reconnect()
    loop_task = asyncio.create_task(connection_manager._reconnection_loop())
    
    # 3. Act: Yield control so the loop starts and gets "stuck" on our mocked open_connection
    await asyncio.sleep(0) 
    
    # Cancel the task explicitly
    loop_task.cancel()
    
    # 4. Assert the CancelledError is correctly raised back to the test
    with pytest.raises(asyncio.CancelledError):
        await loop_task
        
    # 5. Assert: The finally block must have executed our mocked _reset_streams
    mock_reset.assert_called()
     
@pytest.mark.asyncio
async def test_open_connection_invalid_certificate(mocker, connection_manager):
    """Test connection aborts if the server certificate is invalid."""
    # Arrange: Simulate successful network stream creation
    mocker.patch('models.connection_manager.TLSSessionHelper')
    mock_reader = mocker.AsyncMock()
    mock_writer = mocker.Mock()
    mocker.patch(
        'models.connection_manager.asyncio.open_connection', 
        return_value=(mock_reader, mock_writer)
    )
    
    # Arrange: Force the security validation to FAIL
    mocker.patch(
        'models.connection_manager.validate_server_certificate', 
        return_value=False
    )
    
    # Act
    result = await connection_manager._open_connection()
    
    # Assert: The manager must refuse to connect
    assert result is False

@pytest.mark.asyncio
async def test_start_idempotency_prevents_duplicate_loops(mocker, connection_manager):
    """Test that calling start() twice does not spawn duplicate background tasks."""
    # Arrange: Spy on the logger to verify the warning
    mock_logger = mocker.patch('models.connection_manager.logger.warning')
    
    # Arrange: Simulate an already active state by injecting a dummy task
    connection_manager._reconnection_task = mocker.Mock()
    
    # Act: Attempt to start the manager again
    await connection_manager.start()
    
    # Assert: It should log a warning and exit cleanly
    mock_logger.assert_called_once_with("Connection manager already started")
    
    # Assert: Ensure it did NOT try to overwrite the existing task
    # If it had continued, it would have called _set_events_reconnect
    assert connection_manager._connected.is_set() is False