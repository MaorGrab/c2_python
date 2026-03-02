"""
Unit tests for CommunicationManager
Tests with real encryption, mocking only I/O boundary
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock
from models.communication_manager import CommunicationManager
from models.message import Message
from models.message_type import MessageType
from models.command_type import CommandType
from models.encryption_manager import EncryptionManager


@pytest.fixture
def cm():
    """Provide CommunicationManager instance"""
    return CommunicationManager("127.0.0.1", 5000, "test-client")


@pytest.fixture
def peer_em():
    """Provide peer encryption manager for testing"""
    return EncryptionManager()


@pytest.mark.asyncio
async def test_handshake_success(cm, peer_em):
    """Test successful handshake with real encryption"""
    # Mock I/O boundary only
    mock_reader = AsyncMock()
    mock_writer = Mock()
    written_data = []
    
    mock_writer.write = Mock(side_effect=lambda d: written_data.append(d))
    mock_writer.drain = AsyncMock()
    
    cm._connection_manager.reader = mock_reader
    cm._connection_manager.writer = mock_writer
    
    # Mock ACK response with real encryption
    ack_msg = Message.as_ack("test-client", peer_em.public_key_b64)
    ack_payload = ack_msg.to_payload(with_prefix=False)
    ack_length = len(ack_payload).to_bytes(4, 'big')
    
    mock_reader.readexactly = AsyncMock(side_effect=[ack_length, ack_payload])
    
    result = await cm._perform_handshake()
    
    assert result is True
    assert cm._encryption_manager.session_key is not None
    assert len(written_data) == 1
    assert len(written_data[0]) > 4


@pytest.mark.asyncio
async def test_handshake_failure_no_ack(cm):
    """Test handshake failure when no ACK received"""
    mock_reader = AsyncMock()
    mock_writer = Mock()
    mock_writer.write = Mock()
    mock_writer.drain = AsyncMock()
    
    cm._connection_manager.reader = mock_reader
    cm._connection_manager.writer = mock_writer
    
    # Mock empty response
    mock_reader.readexactly = AsyncMock(side_effect=asyncio.IncompleteReadError(b'', 4))
    
    result = await cm._perform_handshake()
    
    assert result is False


@pytest.mark.asyncio
async def test_handshake_invalid_ack_type(cm):
    """Test handshake fails with invalid ACK message type"""
    mock_reader = AsyncMock()
    mock_writer = Mock()
    mock_writer.write = Mock()
    mock_writer.drain = AsyncMock()
    
    cm._connection_manager.reader = mock_reader
    cm._connection_manager.writer = mock_writer
    
    # Send COMMAND instead of ACK
    invalid_msg = Message.as_command("cmd-1", "test")
    invalid_payload = invalid_msg.to_payload(with_prefix=False)
    invalid_length = len(invalid_payload).to_bytes(4, 'big')
    
    mock_reader.readexactly = AsyncMock(side_effect=[invalid_length, invalid_payload])
    
    result = await cm._perform_handshake()
    
    assert result is False


@pytest.mark.asyncio
async def test_listener_receives_command(cm, peer_em):
    """Test listener receives and queues commands with real encryption"""
    # Setup real encryption
    cm._encryption_manager.establish_session_key(peer_em.public_key_b64)
    peer_em.establish_session_key(cm._encryption_manager.public_key_b64)
    
    # Mock I/O boundary
    mock_reader = AsyncMock()
    cm._connection_manager.reader = mock_reader
    
    # Create real encrypted command
    cmd_msg = Message.as_command("cmd-1", "whoami")
    encrypted = peer_em.encrypt(cmd_msg)
    encrypted_data = encrypted[4:]
    length_prefix = len(encrypted_data).to_bytes(4, 'big')
    
    # Mock reader to return encrypted message then raise to exit
    mock_reader.readexactly = AsyncMock(
        side_effect=[length_prefix, encrypted_data, asyncio.IncompleteReadError(b'', 4)]
    )
    
    # Run listener
    with pytest.raises(asyncio.IncompleteReadError):
        await cm._listener()
    
    # Verify command was queued
    assert not cm.command_queue.empty()
    cmd_data = await cm.command_queue.get()
    assert cmd_data["cmd_id"] == "cmd-1"
    assert cmd_data["command"] == "whoami"


@pytest.mark.asyncio
async def test_listener_handles_kill_command(cm, peer_em):
    """Test listener handles KILL command with real encryption"""
    # Setup real encryption
    cm._encryption_manager.establish_session_key(peer_em.public_key_b64)
    peer_em.establish_session_key(cm._encryption_manager.public_key_b64)
    
    mock_reader = AsyncMock()
    cm._connection_manager.reader = mock_reader
    cm._connection_manager.shutdown = AsyncMock()
    
    # Create real encrypted KILL command
    kill_msg = Message.as_command("cmd-kill", CommandType.KILL.value)
    encrypted = peer_em.encrypt(kill_msg)
    encrypted_data = encrypted[4:]
    length_prefix = len(encrypted_data).to_bytes(4, 'big')
    
    mock_reader.readexactly = AsyncMock(side_effect=[length_prefix, encrypted_data])
    
    await cm._listener()
    
    # Verify queues were drained and kill result added
    assert cm.command_queue.empty()
    assert not cm.result_queue.empty()


@pytest.mark.asyncio
async def test_listener_exits_on_empty_message(cm, peer_em):
    """Test listener exits gracefully on connection close"""
    cm._encryption_manager.establish_session_key(peer_em.public_key_b64)
    
    mock_reader = AsyncMock()
    cm._connection_manager.reader = mock_reader
    
    # Mock connection close
    mock_reader.readexactly = AsyncMock(side_effect=asyncio.IncompleteReadError(b'', 4))
    
    # Should exit without exception
    with pytest.raises(asyncio.IncompleteReadError):
        await cm._listener()


@pytest.mark.asyncio
async def test_talker_sends_results(cm, peer_em):
    """Test talker sends results with real encryption"""
    # Setup real encryption
    cm._encryption_manager.establish_session_key(peer_em.public_key_b64)
    peer_em.establish_session_key(cm._encryption_manager.public_key_b64)
    
    # Mock I/O boundary
    mock_writer = Mock()
    written_data = []
    
    mock_writer.write = Mock(side_effect=lambda d: written_data.append(d))
    mock_writer.drain = AsyncMock()
    
    cm._connection_manager.writer = mock_writer
    
    # Add result to queue
    await cm.result_queue.put(("cmd-1", "output", 50.0))
    
    # Run talker briefly
    talker_task = asyncio.create_task(cm._talker())
    await asyncio.sleep(0.1)
    talker_task.cancel()
    
    with pytest.raises(asyncio.CancelledError):
        await talker_task
    
    # Verify encrypted message was written
    assert len(written_data) == 1
    encrypted_msg = written_data[0]
    
    # Verify we can decrypt it
    decrypted = peer_em.decrypt(encrypted_msg[4:])
    assert decrypted is not None
    assert decrypted.type == MessageType.RESULT
    assert decrypted.cmd_id == "cmd-1"
    assert decrypted.result == "output"


@pytest.mark.asyncio
async def test_cleanup_tasks_cancels_properly(cm):
    """Test that cleanup cancels tasks properly"""
    cm._listener_task = asyncio.create_task(asyncio.sleep(10))
    cm._talker_task = asyncio.create_task(asyncio.sleep(10))
    cm._active.set()
    
    await cm._cleanup_tasks()
    await asyncio.sleep(0.01)
    
    assert not cm.is_active
    assert cm._listener_task.done()
    assert cm._talker_task.done()


@pytest.mark.asyncio
async def test_handle_kill_drains_queues(cm):
    """Test that kill command drains queues"""
    await cm.command_queue.put({"cmd_id": "1", "command": "test"})
    
    cm._connection_manager.shutdown = AsyncMock()
    
    await cm._handle_kill_command()
    
    assert cm.command_queue.empty()
    assert not cm.result_queue.empty()
