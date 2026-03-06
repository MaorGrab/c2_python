import pytest
import asyncio
from unittest.mock import AsyncMock, Mock

from c2_python.src.models.message import Message
from c2_python.src.models.message_type import MessageType
from c2_python.src.models.command_type import CommandType

# --- 1. HANDSHAKE TESTS ---

@pytest.mark.asyncio
async def test_handshake_success(mocker, communication_manager):
    """Test successful handshake logic sets session key."""
    # Arrange: Mock send success
    mocker.patch('models.communication_manager.send_message', new_callable=AsyncMock, return_value=True)
    
    # Arrange: Mock receive success with valid ACK
    mock_ack_msg = Message.as_ack("server", "server_public_key")
    mocker.patch('models.communication_manager.receive_message', new_callable=AsyncMock, return_value="mock_payload")
    mocker.patch('models.communication_manager.Message.from_payload', return_value=mock_ack_msg)
    
    # Act
    result = await communication_manager._perform_handshake()
    
    # Assert
    assert result is True
    communication_manager._encryption_manager.establish_session_key.assert_called_once_with("server_public_key")

@pytest.mark.asyncio
async def test_handshake_fails_on_invalid_ack(mocker, communication_manager):
    """Test handshake fails if server returns something other than an ACK."""
    mocker.patch('models.communication_manager.send_message', new_callable=AsyncMock, return_value=True)
    mocker.patch('models.communication_manager.receive_message', new_callable=AsyncMock, return_value="mock_payload")
    
    # Mock a COMMAND message instead of an ACK
    invalid_msg = Message.as_command("cmd-1", "whoami")
    mocker.patch('models.communication_manager.Message.from_payload', return_value=invalid_msg)
    
    result = await communication_manager._perform_handshake()
    
    assert result is False
    communication_manager._encryption_manager.establish_session_key.assert_not_called()


# --- 2. LISTENER TESTS ---

@pytest.mark.asyncio
async def test_listener_routes_commands_and_skips_non_commands(mocker, communication_manager):
    """Test listener drops non-command messages (like stray ACKs) but queues valid ones."""
    # Arrange: We only test the valid decryption path here
    mock_non_cmd = Mock(type=MessageType.ACK)
    mock_valid_cmd = Mock(type=MessageType.COMMAND, cmd_id="123", command="whoami")
    
    # Setup the receive loop to yield 2 items, then gracefully exit with None
    mocker.patch(
        'models.communication_manager.receive_message', 
        new_callable=AsyncMock, 
        side_effect=["payload_ack", "payload_cmd", None]
    )
    
    communication_manager._encryption_manager.decrypt.side_effect = [
        mock_non_cmd, 
        mock_valid_cmd
    ]
    
    # Act
    await communication_manager._listener()
    
    # Assert: Only the valid command made it to the queue
    assert communication_manager.command_queue.qsize() == 1
    queued_item = await communication_manager.command_queue.get()
    assert queued_item["cmd_id"] == "123"
    assert queued_item["command"] == "whoami"


@pytest.mark.asyncio
async def test_listener_aborts_on_decryption_failure(mocker, communication_manager):
    """Test listener intentionally breaks the loop to force a reconnection if decryption fails."""
    # Arrange: Mock a received message
    mocker.patch(
        'models.communication_manager.receive_message', 
        new_callable=AsyncMock, 
        return_value="corrupted_payload"
    )
    
    # Arrange: Force decryption to fail
    communication_manager._encryption_manager.decrypt.return_value = None
    
    # Spy on the logger to ensure the warning fires
    mock_logger = mocker.patch('models.communication_manager.logger.warning')
    
    # Act
    await communication_manager._listener()
    
    # Assert: The listener exited immediately without queuing anything
    assert communication_manager.command_queue.empty() is True
    mock_logger.assert_called_once_with("Failed to decrypt message")


@pytest.mark.asyncio
async def test_listener_handles_kill_command(mocker, communication_manager):
    """Test the KILL command executes specific teardown logic."""
    # Arrange
    kill_cmd = Mock(type=MessageType.COMMAND, cmd_id="666", command=CommandType.KILL.value)
    
    mocker.patch('models.communication_manager.receive_message', new_callable=AsyncMock, return_value="payload")
    communication_manager._encryption_manager.decrypt.return_value = kill_cmd
    
    mock_handle_kill = mocker.patch.object(communication_manager, '_handle_kill_command', new_callable=AsyncMock)
    
    # Act
    await communication_manager._listener()
    
    # Assert: The listener loop must break after a KILL command
    mock_handle_kill.assert_awaited_once()


# --- 3. TALKER TESTS ---

@pytest.mark.asyncio
async def test_talker_encrypts_and_sends(mocker, communication_manager):
    """Test that the talker successfully pulls from queue, encrypts, and transmits."""
    # Arrange
    await communication_manager.result_queue.put(("cmd-99", "success output", 15.5))
    
    communication_manager._encryption_manager.encrypt.return_value = "encrypted_bytes"
    mock_send = mocker.patch('models.communication_manager.send_message', new_callable=AsyncMock, return_value=True)
    
    # Act: Start talker in background
    talker_task = asyncio.create_task(communication_manager._talker())
    
    # Let the event loop cycle once to process the queue
    await asyncio.sleep(0) 
    
    # Cleanup: Cancel task
    talker_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await talker_task
        
    # Assert
    mock_send.assert_awaited_once()
    communication_manager._encryption_manager.encrypt.assert_called_once()


# --- 4. ORCHESTRATION & STATE MACHINE TESTS ---

@pytest.mark.asyncio
async def test_start_communication_shuts_down_on_handshake_failure(mocker, communication_manager):
    """Test that if wait_connected passes but handshake fails, the manager shuts down."""
    # Arrange: Connection works, Handshake fails
    communication_manager._connection_manager.wait_connected.side_effect = [True, False]
    mocker.patch.object(communication_manager, '_perform_handshake', new_callable=AsyncMock, return_value=False)
    
    mock_start_loop = mocker.patch.object(communication_manager, '_start_message_loop', new_callable=AsyncMock)
    
    # Act
    await communication_manager.start_communication()
    
    # Assert: Must NOT start message loop, MUST call shutdown
    mock_start_loop.assert_not_called()
    communication_manager._connection_manager.shutdown.assert_awaited_once()


@pytest.mark.asyncio
async def test_handle_kill_command_drains_queues_and_injects_poison_pill(mocker, communication_manager):
    """Test _handle_kill_command properly sanitizes state and sends final result."""
    # Arrange: Pre-fill queues
    await communication_manager.command_queue.put("old_cmd")
    await communication_manager.result_queue.put("old_result")
    
    # Act
    await communication_manager._handle_kill_command()
    
    # Assert: Old data is gone
    assert communication_manager.command_queue.empty() is True
    
    # Assert: Poison pill was injected
    assert communication_manager.result_queue.qsize() == 1
    final_result = await communication_manager.result_queue.get()
    assert final_result == ('666', 'KILLED BY SERVER', 0)
    
    # Assert: Connection manager instructed to shutdown
    communication_manager._connection_manager.shutdown.assert_awaited_once()

@pytest.mark.asyncio
async def test_start_communication_triggers_reconnection_on_connection_loss(mocker, communication_manager):
    """Test that if the message loop finishes (e.g., due to connection loss), it triggers a reconnection."""
    # Arrange: Simulate the connection manager being connected twice, then disconnected
    communication_manager._connection_manager.wait_connected.side_effect = [True, True, False]
    
    # Arrange: Handshake always succeeds
    mocker.patch.object(communication_manager, '_perform_handshake', new_callable=AsyncMock, return_value=True)
    
    # Arrange: Simulate the message loop running, but returning (simulating a dropped connection)
    mock_msg_loop = mocker.patch.object(communication_manager, '_start_message_loop', new_callable=AsyncMock)
    
    # Act
    await communication_manager.start_communication()
    
    # Assert: It should have started the message loop twice
    assert mock_msg_loop.call_count == 2
    
    # Assert: It MUST have triggered reconnection twice to keep the agent alive
    assert communication_manager._connection_manager.trigger_reconnection.call_count == 2
    
    # Assert: Once wait_connected returned False, it shut down cleanly
    communication_manager._connection_manager.shutdown.assert_awaited_once()

@pytest.mark.asyncio
async def test_talker_continues_on_send_failure(mocker, communication_manager):
    """Test that a failure to send one result does not crash the talker loop."""
    # Arrange: Queue two results
    await communication_manager.result_queue.put(("cmd-1", "failed_result", 10.0))
    await communication_manager.result_queue.put(("cmd-2", "success_result", 12.0))
    
    communication_manager._encryption_manager.encrypt.return_value = b"encrypted_dummy"
    
    # Simulate the first send failing, and the second succeeding
    mocker.patch(
        'models.communication_manager.send_message', 
        new_callable=AsyncMock, 
        side_effect=[False, True]
    )
    mock_logger = mocker.patch('models.communication_manager.logger.warning')
    
    # Act: Start talker
    talker_task = asyncio.create_task(communication_manager._talker())
    
    # Yield control to the event loop so the talker can process both items
    await asyncio.sleep(0.01) 
    
    # Cleanup
    talker_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await talker_task
        
    # Assert: Both items were pulled from the queue
    assert communication_manager.result_queue.empty() is True
    
    # Assert: The warning was logged for the first failure
    mock_logger.assert_called_once_with("[cmd id: cmd-1] Failed to send result")

@pytest.mark.asyncio
async def test_listener_breaks_on_empty_message(mocker, communication_manager):
    """Test that a dropped TCP connection (empty message) cleanly exits the listener."""
    # Arrange: Simulate a silent network drop
    mocker.patch(
        'models.communication_manager.receive_message', 
        new_callable=AsyncMock, 
        return_value=None
    )
    
    # Spy on the logger to verify the exact code path
    mock_logger = mocker.patch('models.communication_manager.logger.warning')
    
    # Act
    await communication_manager._listener()
    
    # Assert: The loop broke without erroring out
    mock_logger.assert_called_once_with("Received empty message")
    communication_manager._encryption_manager.decrypt.assert_not_called()