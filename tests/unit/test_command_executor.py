import pytest
import asyncio
from unittest.mock import AsyncMock, Mock

# --- 1. SUBPROCESS & TERMINATION EDGE CASES ---

@pytest.mark.asyncio
async def test_terminate_process_falls_back_to_forceful_on_timeout(mocker, executor):
    """Test that a timeout during graceful shutdown triggers a forceful kill."""
    mock_process = mocker.Mock()
    mock_process.pid = 9999
    executor.execution_process = mock_process
    
    mock_graceful = mocker.patch(
        'models.command_executor.subprocess_helper.terminate_subprocess_gracefully',
        new_callable=AsyncMock,
        side_effect=asyncio.TimeoutError()
    )
    mock_forceful = mocker.patch(
        'models.command_executor.subprocess_helper.terminate_subprocess_forcefully',
        new_callable=AsyncMock
    )
    
    await executor._terminate_execution_process()
    
    mock_graceful.assert_awaited_once_with(mock_process)
    mock_forceful.assert_awaited_once_with(mock_process)
    assert executor.execution_process is None


@pytest.mark.asyncio
async def test_execute_command_cleans_up_on_cancellation(mocker, executor):
    """Test that if execution is cancelled mid-flight, the process is terminated."""
    mock_process = mocker.Mock()
    mocker.patch(
        'models.command_executor.subprocess_helper.create_subprocess',
        new_callable=AsyncMock,
        return_value=mock_process
    )
    mocker.patch(
        'models.command_executor.subprocess_helper.communicate_subprocess',
        new_callable=AsyncMock,
        side_effect=asyncio.CancelledError()
    )
    mock_terminate = mocker.patch.object(
        executor, '_terminate_execution_process', 
        new_callable=AsyncMock
    )
    
    with pytest.raises(asyncio.CancelledError):
        await executor._execute_command("cmd-1", "fake_command")
        
    mock_terminate.assert_awaited_once()


@pytest.mark.asyncio
async def test_execute_command_handles_standard_exceptions(mocker, executor):
    """Test that general exceptions are caught and formatted as string errors."""
    error_message = "Permission denied"
    mocker.patch(
        'models.command_executor.subprocess_helper.create_subprocess',
        new_callable=AsyncMock,
        side_effect=PermissionError(error_message)
    )
    
    result = await executor._execute_command("cmd-2", "bad_command")
    
    assert result == f"Error: {error_message}"
    assert executor.execution_process is None


# --- 2. EXECUTOR LOOP & QUEUE MECHANICS ---

@pytest.mark.asyncio
async def test_start_processes_valid_command_and_queues_result(mocker, executor, command_queues):
    """Test that the loop successfully pulls a command, executes it, and outputs the result."""
    in_queue, out_queue = command_queues
    
    # Arrange: Mock the internal processor so we don't touch the OS
    mock_process_command = mocker.patch.object(
        executor, '_process_command',
        new_callable=AsyncMock,
        return_value=("cmd-1", "success_output", 42.5)
    )
    
    # Arrange: Put a standard command in the queue
    await in_queue.put({"cmd_id": "cmd-1", "command": "echo hello"})
    
    # Act: Start the executor loop as a background task
    executor_task = asyncio.create_task(executor.start(in_queue, out_queue))
    
    # Assert: Wait for the result in the out_queue
    cmd_id, result, exec_time = await asyncio.wait_for(out_queue.get(), timeout=1.0)
    
    assert cmd_id == "cmd-1"
    assert result == "success_output"
    assert exec_time == 42.5
    mock_process_command.assert_awaited_once_with(("cmd-1", "echo hello"))
    
    # Cleanup: Stop the loop safely
    executor.stop()
    executor_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await executor_task


@pytest.mark.asyncio
async def test_start_skips_none_in_queue(mocker, executor, command_queues):
    """Test that receiving None in the queue logs a warning and continues."""
    in_queue, out_queue = command_queues
    
    mock_logger = mocker.patch('models.command_executor.logger.warning')
    mocker.patch.object(
        executor, '_process_command',
        new_callable=AsyncMock,
        return_value=("cmd-2", "valid", 10.0)
    )
    
    # Arrange: Inject None, followed by a valid command
    await in_queue.put(None)
    await in_queue.put({"cmd_id": "cmd-2", "command": "echo test"})
    
    executor_task = asyncio.create_task(executor.start(in_queue, out_queue))
    
    # Act: Retrieve the next output
    cmd_id, _, _ = await asyncio.wait_for(out_queue.get(), timeout=1.0)
    
    # Assert: The loop survived the None and processed cmd-2
    assert cmd_id == "cmd-2"
    mock_logger.assert_called_once_with("Received None command")
    
    # Cleanup
    executor.stop()
    executor_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await executor_task


@pytest.mark.asyncio
async def test_stop_flag_halts_execution_loop(mocker, executor, command_queues):
    """Test that the _should_stop flag correctly breaks the while loop."""
    in_queue, out_queue = command_queues
    
    # Set the flag before starting
    executor.stop()
    
    # Run the start loop directly (not as a background task)
    # If the flag works, this will return instantly instead of hanging on in_queue.get()
    try:
        await asyncio.wait_for(executor.start(in_queue, out_queue), timeout=0.5)
    except asyncio.TimeoutError:
        pytest.fail("Executor loop failed to honor the _should_stop flag and hung.")
        
    assert executor.is_killed is True