import pytest
import asyncio
from unittest.mock import AsyncMock, Mock, call

from c2_python.src.models.message_type import MessageType
from c2_python.src.models.command_type import CommandType

# --- 1. STATE & QUEUE MANAGEMENT ---

def test_add_command_generates_uuid_and_queues(client_state):
    """Test that adding a command assigns a UUID and places it in the queue."""
    cmd_id = client_state.add_command("whoami")
    
    assert isinstance(cmd_id, str)
    assert len(cmd_id) > 10 # Basic UUID check
    assert client_state.command_queue.qsize() == 1
    
    queued_item = client_state.command_queue.get_nowait()
    assert queued_item["cmd_id"] == cmd_id
    assert queued_item["command"] == "whoami"

def test_kill_drains_queues_and_sets_status(client_state):
    """Test the kill() method cleanly flushes old state and injects the KILL command."""
    # Arrange: Pollute the state
    client_state.pending_results["old_cmd"] = "running"
    client_state.add_command("dir")
    
    assert client_state.command_queue.qsize() == 1
    assert len(client_state.pending_results) == 1
    
    # Act
    result = client_state.kill()
    
    # Assert
    assert result is True
    assert client_state.is_killed is True
    assert len(client_state.pending_results) == 0
    
    # The queue should have been drained of 'dir', and only 'KILL' should remain
    assert client_state.command_queue.qsize() == 1
    queued_item = client_state.command_queue.get_nowait()
    assert queued_item["command"] == CommandType.KILL.value


# --- 2. THE EXECUTOR LOOP ---

@pytest.mark.asyncio
async def test_command_executor_encrypts_and_sends(mocker, client_state):
    """Test that the executor pulls from the queue, tracks pending state, and transmits."""
    # Arrange: Queue a command
    cmd_id = client_state.add_command("ipconfig")
    
    # Mock transmission
    mock_send = mocker.patch('models.client_state.send_message', new_callable=AsyncMock)
    
    # Act: Start the executor loop
    executor_task = asyncio.create_task(client_state._command_executor())
    
    # Yield control to let the loop process the queue
    await asyncio.sleep(0)  # pass control to other coroutines
    
    # Cleanup: 1. Stop the loop by changing state
    client_state.set_disconnected()
    
    # Cleanup: 2. UNBLOCK THE QUEUE! 
    # This wakes up the waiting task, forces it to evaluate the `if cmd_data is None: continue` logic,
    # and sends it back to the top of the `while` loop, which will now evaluate to False and exit.
    client_state.command_queue.put_nowait(None)
    
    # Now it will safely finish without hanging
    await executor_task
    
    # Assert: Queue is empty
    assert client_state.command_queue.empty() is True
    
    # Assert: The command was added to pending results tracking
    assert client_state.pending_results[cmd_id] == "ipconfig"
    
    # Assert: Message was sent
    mock_send.assert_awaited_once()


@pytest.mark.asyncio
async def test_executor_exits_cleanly_on_disconnect(client_state):
    """Test that changing status breaks the while loop gracefully."""
    # Start the loop while connected
    assert client_state.is_connected is True
    executor_task = asyncio.create_task(client_state._command_executor())
    
    # Change status
    client_state.set_disconnected()
    
    # Await the task. If it hangs, the test will timeout. 
    # Because we changed the status, the `while self.is_connected:` should break.
    # To force the queue.get() to release, we must inject the sentinel (None)
    client_state.command_queue.put_nowait(None)
    
    await asyncio.wait_for(executor_task, timeout=0.5)
    assert executor_task.done()


# --- 3. THE RECEIVER LOOP & DISCONNECTION HANDLING ---

@pytest.mark.asyncio
async def test_receiver_clears_pending_on_valid_result(mocker, client_state):
    """Test that a valid result message removes the command from pending_results."""
    # Arrange: Mock a pending command
    client_state.pending_results["cmd-123"] = "whoami"
    
    # Arrange: Mock receiving a valid result message
    mock_result_msg = Mock(type=MessageType.RESULT, cmd_id="cmd-123", result="root")
    mocker.patch(
        'models.client_state.receive_message', 
        new_callable=AsyncMock, 
        side_effect=[b"raw_data", b""] 
    )
    client_state._encryption_manager.decrypt.return_value = mock_result_msg
    
    # Act: Run receiver loop
    receiver_task = asyncio.create_task(client_state._message_receiver())
    await asyncio.sleep(0)  # pass control to other coroutines
    
    # Assert: The pending result tracker was cleared
    assert "cmd-123" not in client_state.pending_results
    
    # Cleanup
    client_state.set_disconnected()
    client_state.command_queue.put_nowait(None)
    await receiver_task


@pytest.mark.asyncio
async def test_receiver_handles_network_disconnect(mocker, client_state):
    """Test that an IncompleteReadError properly triggers the disconnection handler."""
    # Arrange: Simulate connection drop
    mocker.patch(
        'models.client_state.receive_message', 
        new_callable=AsyncMock, 
        side_effect=asyncio.IncompleteReadError(b'', None)
    )
    
    # Spy on the handler
    mock_handler = mocker.patch.object(client_state, '_handle_disconnection')
    
    # Act
    await client_state._message_receiver()
    
    # Assert
    mock_handler.assert_called_once()


# --- 4. ORCHESTRATION & LIFECYCLE CLEANUP ---

@pytest.mark.asyncio
async def test_run_lifecycle_cleans_up_on_cancellation(mocker, client_state):
    """Test that if the lifecycle is cancelled, it safely cleans up tasks and sockets."""
    # Arrange: Mock the internal components so they run indefinitely
    mocker.patch.object(client_state, '_message_receiver', new_callable=AsyncMock)
    mocker.patch.object(client_state, '_command_executor', new_callable=AsyncMock)
    
    # Spy on the cleanups
    mock_cleanup_tasks = mocker.patch.object(client_state, '_cleanup_tasks', new_callable=AsyncMock)
    mock_cleanup_conn = mocker.patch.object(client_state, '_cleanup_connection', new_callable=AsyncMock)
    
    # Act: Start the lifecycle
    lifecycle_task = asyncio.create_task(client_state.run_lifecycle())
    await asyncio.sleep(0)  # pass control to other coroutines
    
    # Cancel the lifecycle
    lifecycle_task.cancel()
    
    # The try/except CancelledError should absorb this, so it won't raise
    await lifecycle_task
    
    # Assert: Finally block executed
    mock_cleanup_tasks.assert_awaited_once()
    mock_cleanup_conn.assert_awaited_once()

@pytest.mark.asyncio
async def test_receiver_ignores_untracked_results(mocker, client_state):
    """Test that receiving a result for an unknown command ID does not crash the server."""
    # Arrange: Ensure pending_results is completely empty
    client_state.pending_results.clear()
    
    # Arrange: Mock a result for a ghost command
    mock_result_msg = Mock(type=MessageType.RESULT, cmd_id="ghost-999", result="data")
    
    # Use our one-two punch to run the loop exactly once
    mocker.patch(
        'models.client_state.receive_message', 
        new_callable=AsyncMock, 
        side_effect=[b"data", b""]
    )
    client_state._encryption_manager.decrypt.return_value = mock_result_msg
    
    # Act
    await client_state._message_receiver()
    
    # Assert: The loop finished cleanly without throwing a KeyError
    assert "ghost-999" not in client_state.pending_results

@pytest.mark.asyncio
async def test_receiver_skips_wrong_message_types(mocker, client_state):
    """Test that the server ignores commands sent BY the client."""
    # Arrange: Client sends a COMMAND (illegal for a client to do)
    mock_bad_msg = Mock(type=MessageType.COMMAND) 
    
    mocker.patch(
        'models.client_state.receive_message', 
        new_callable=AsyncMock, 
        side_effect=[b"data", b""]
    )
    client_state._encryption_manager.decrypt.return_value = mock_bad_msg
    mock_logger = mocker.patch('models.client_state.logger.warning')

    # Act
    await client_state._message_receiver()

    # Assert: The warning was logged and the loop survived
    mock_logger.assert_has_calls([
        call(f"[{client_state.client_id}] Unknown message type: {MessageType.COMMAND}"),
        call(f"[{client_state.client_id}] Empty message received")
    ])

def test_handle_disconnection_is_idempotent(client_state):
    """Test that simultaneous disconnect triggers do not corrupt the state or queues."""
    # Act 1: First disconnect
    client_state._handle_disconnection()
    
    # Assert 1: State is changed and ONE sentinel is in the queue
    assert client_state.is_disconnected is True
    assert client_state.command_queue.qsize() == 1
    
    # Act 2: Second disconnect trigger (simulating a race condition)
    client_state._handle_disconnection()
    
    # Assert 2: The queue MUST STILL only have one sentinel
    assert client_state.command_queue.qsize() == 1

@pytest.mark.asyncio
async def test_cleanup_connection_wipes_pointers_on_timeout(mocker, client_state):
    """Test that a hanging socket closure still results in memory cleanup."""
    # Arrange: Make wait_closed hang forever
    client_state.writer.wait_closed = AsyncMock(side_effect=asyncio.TimeoutError())
    
    # Spy on the logger to verify the exact branch
    mock_logger = mocker.patch('models.client_state.logger.info')
    
    # Act
    await client_state._cleanup_connection()
    
    # Assert: The timeout branch was hit
    mock_logger.assert_any_call(f"[{client_state.client_id}] Timeout closing connection")
    
    # Assert: The finally block executed and wiped the pointers to prevent leaks
    assert client_state.reader is None
    assert client_state.writer is None