import pytest
from unittest.mock import AsyncMock

from models.protocol.message import Message

# --- 1. REGISTRATION LOGIC (THE FACTORY) ---

@pytest.mark.asyncio
async def test_register_client_success(mocker, client_manager, mock_streams):
    """Test successful client registration, encryption setup, and dict mapping."""
    reader, writer = mock_streams
    client_id = "agent-007"
    client_public_key = "client_key_b64"
    server_public_key = "server_key_b64"
    
    # Arrange: Mock incoming registration message
    mock_reg_msg = Message.as_register(client_id, client_public_key)
    mocker.patch('models.server.client_manager.receive_message', new_callable=AsyncMock, return_value=b"raw_data")
    mocker.patch('models.server.client_manager.Message.from_payload', return_value=mock_reg_msg)
    
    # Arrange: Mock the ClientState initialization so we don't trigger real crypto
    mock_client_state = mocker.Mock()
    mock_client_state.setup_encryption.return_value = server_public_key
    mocker.patch('models.server.client_manager.ClientState', return_value=mock_client_state)
    
    # Arrange: Mock outgoing ACK
    mock_send = mocker.patch('models.server.client_manager.send_message', new_callable=AsyncMock)
    
    # Act
    result = await client_manager.register_client(reader, writer)
    
    # Assert: Client was created and tracked
    assert result is mock_client_state
    assert client_manager.clients[client_id] is mock_client_state
    
    # Assert: Encryption was seeded correctly
    mock_client_state.setup_encryption.assert_called_once_with(client_public_key)
    
    # Assert: ACK was sent
    mock_send.assert_awaited_once()

@pytest.mark.asyncio
async def test_register_client_rejects_invalid_message_type(mocker, client_manager, mock_streams):
    """Test registration aborts and closes socket if the first message is not a REGISTER type."""
    reader, writer = mock_streams
    
    # Arrange: Mock a rogue COMMAND message instead of a REGISTER message
    rogue_msg = Message.as_command("cmd-1", "whoami")
    mocker.patch('models.server.client_manager.receive_message', new_callable=AsyncMock, return_value=b"raw_data")
    mocker.patch('models.server.client_manager.Message.from_payload', return_value=rogue_msg)
    
    # Act
    result = await client_manager.register_client(reader, writer)
    
    # Assert: Registration failed, client not tracked, socket closed
    assert result is None
    assert len(client_manager.clients) == 0
    writer.close.assert_called_once()

@pytest.mark.asyncio
async def test_register_client_handles_receive_exception(mocker, client_manager, mock_streams):
    """Test registration survives an unexpected network drop during handshake."""
    reader, writer = mock_streams
    
    # Arrange: Force receive_message to throw an error
    mocker.patch('models.server.client_manager.receive_message', new_callable=AsyncMock, side_effect=ConnectionResetError)
    
    # Act
    result = await client_manager.register_client(reader, writer)
    
    # Assert
    assert result is None
    assert len(client_manager.clients) == 0


# --- 2. DELEGATION METHODS ---

def test_add_command_to_queue_delegates_to_client(mocker, client_manager):
    """Test that commands are properly routed to the correct ClientState object."""
    # Arrange: Inject a mock client
    mock_client = mocker.Mock()
    mock_client.add_command.return_value = "uuid-123"
    client_manager.clients["agent-1"] = mock_client
    
    # Act
    success = client_manager.add_command_to_queue("agent-1", "ipconfig")
    
    # Assert: Routed correctly
    assert success is True
    mock_client.add_command.assert_called_once_with("ipconfig")
    
    # Act/Assert: Non-existent client
    assert client_manager.add_command_to_queue("ghost", "ipconfig") is False

def test_add_command_catches_client_exceptions(mocker, client_manager):
    """Test safety net if ClientState.add_command throws an exception."""
    # Arrange: Inject a broken client
    mock_client = mocker.Mock()
    mock_client.add_command.side_effect = Exception("Queue full")
    client_manager.clients["agent-1"] = mock_client
    
    # Act
    success = client_manager.add_command_to_queue("agent-1", "ipconfig")
    
    # Assert: Handled gracefully
    assert success is False

def test_kill_client_delegates_to_client(mocker, client_manager):
    """Test that the kill command is routed correctly."""
    mock_client = mocker.Mock()
    mock_client.kill.return_value = True
    client_manager.clients["agent-1"] = mock_client
    
    # Act
    success = client_manager.kill_client("agent-1")
    
    # Assert
    assert success is True
    mock_client.kill.assert_called_once()
    assert client_manager.kill_client("ghost") is False


# --- 3. LIFECYCLE & TEARDOWN ---

@pytest.mark.asyncio
async def test_handle_client_loop_delegation(mocker, client_manager):
    """Test that the manager correctly spins up the client's internal loop."""
    mock_client = mocker.Mock()
    mock_client.run_lifecycle = AsyncMock()
    
    # Act
    await client_manager.handle_client_loop(mock_client)
    
    # Assert
    mock_client.run_lifecycle.assert_awaited_once()

@pytest.mark.asyncio
async def test_close_all_clients_triggers_cleanup_and_clears_registry(mocker, client_manager):
    """Test that server shutdown forces all connected clients to clean up sockets."""
    # Arrange: Inject two mock clients
    mock_client_1 = mocker.Mock()
    mock_client_1._cleanup_connection = AsyncMock()
    mock_client_2 = mocker.Mock()
    mock_client_2._cleanup_connection = AsyncMock()
    
    client_manager.clients["agent-1"] = mock_client_1
    client_manager.clients["agent-2"] = mock_client_2
    
    # Act
    await client_manager.close_all_clients()
    
    # Assert: Both were cleaned up
    mock_client_1._cleanup_connection.assert_awaited_once()
    mock_client_2._cleanup_connection.assert_awaited_once()
    
    # Assert: Registry is empty
    assert len(client_manager.clients) == 0