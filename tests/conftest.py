import pytest_asyncio
import pytest
import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, Mock

# Add src directory to Python path for imports
src_path = Path(__file__).parent.parent / 'src'
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from models.client.connection_manager import ConnectionManager
from models.client.command_executor import CommandExecutor
from models.client.communication_manager import CommunicationManager
from models.server.client_state import ClientState
from models.protocol.connection_status import ConnectionStatus
from models.server.client_manager import ClientManager
from models.security.encryption_manager import EncryptionManager
from models.protocol.message import Message
from models.protocol.message_type import MessageType

@pytest_asyncio.fixture
async def connection_manager():
    """
    Provides a cleanly initialized ConnectionManager instance for each test.
    Ensures safe teardown of asyncio tasks regardless of test outcome.
    """
    # 1. SETUP (Arrange)
    # Instantiate with test-safe, deterministic dummy variables
    manager = ConnectionManager(
        server_host="127.0.0.1",
        server_port=8888,
        client_id="test_client_001"
    )
    
    # 2. EXECUTION
    # Pause the fixture and hand the manager instance over to the test
    yield manager
    
    # 3. TEARDOWN (Cleanup)
    # This block executes AFTER the test finishes (whether it passed or failed).
    # It guarantees that if a test started the reconnection loop, it is safely 
    # shut down before the next test begins.
    if manager.is_active:
        await manager.shutdown()
        
    # Extra safety net: ensure streams are cleared if the test didn't do it
    await manager._reset_streams()


@pytest_asyncio.fixture
async def executor():
    """
    Provides a cleanly initialized CommandExecutor instance.
    Ensures the executor is stopped after each test to prevent background loop leaks.
    """
    cmd_executor = CommandExecutor()
    
    yield cmd_executor
    
    # Teardown: Stop the loop and ensure no dangling processes remain
    cmd_executor.stop()
    if cmd_executor.execution_process:
        # In a real teardown, we'd mock this, but safely nullifying it here
        # prevents accidental OS leaks if a test failed mid-execution.
        cmd_executor.execution_process = None


@pytest_asyncio.fixture
async def command_queues():
    """
    Provides a tuple of (in_queue, out_queue) for testing the executor loop.
    """
    in_queue = asyncio.Queue()
    out_queue = asyncio.Queue()
    return in_queue, out_queue

@pytest_asyncio.fixture
async def communication_manager(mocker):
    """
    Provides a completely isolated CommunicationManager.
    Network and Encryption layers are strictly mocked.
    """
    # 1. Instantiate the manager
    manager = CommunicationManager("127.0.0.1", 8080, "test-client")
    
    # 2. Inject Mocked Dependencies
    manager._connection_manager = mocker.Mock()
    # Async methods on the connection manager need AsyncMocks
    manager._connection_manager.start = AsyncMock()
    manager._connection_manager.shutdown = AsyncMock()
    manager._connection_manager.wait_connected = AsyncMock()
    manager._connection_manager.trigger_reconnection = AsyncMock()
    
    manager._encryption_manager = mocker.Mock()
    manager._encryption_manager.public_key_b64 = "mock_public_key"
    
    yield manager
    
    # 3. Teardown
    manager._active.clear()
    await manager._cleanup_tasks()

@pytest_asyncio.fixture
async def client_state(mocker, mock_streams):
    """
    Provides an isolated ClientState instance with mocked streams.
    """
    mock_reader, mock_writer = mock_streams
    
    # Instantiate the client
    client = ClientState("test-client-001", mock_reader, mock_writer)
    
    # Mock encryption to bypass crypto overhead
    client._encryption_manager = mocker.Mock()
    client._encryption_manager.public_key_b64 = "server_mock_key"
    client._encryption_manager.encrypt.return_value = b"encrypted_mock"
    
    yield client
    
    # Teardown: ensure cleanup runs if a test failed mid-execution
    client.status = ConnectionStatus.DISCONNECTED
    if client.writer and not client.writer.is_closing():
        client.writer.close()

@pytest.fixture
def client_manager():
    """Provides a fresh ClientManager instance."""
    return ClientManager()

@pytest.fixture
def mock_streams(mocker):
    """Provides mocked asyncio reader and writer for registration testing."""
    writer = mocker.AsyncMock()
    writer.write = mocker.Mock(return_value=None)
    writer.close = mocker.Mock(return_value=None)
    writer.is_closing = mocker.Mock(return_value=False)
    reader = mocker.AsyncMock()
    reader.feed_eof = mocker.Mock()
    return reader, writer

@pytest.fixture
def encryption_manager():
    """Provides a fresh, uninitialized EncryptionManager."""
    return EncryptionManager()

@pytest.fixture
def em_pair():
    """
    Provides a tuple of two EncryptionManagers (Alice and Bob) 
    that have already successfully exchanged public keys and established session keys.
    """
    alice = EncryptionManager()
    bob = EncryptionManager()
    
    # Perform the simulated handshake
    alice.establish_session_key(bob.public_key_b64)
    bob.establish_session_key(alice.public_key_b64)
    
    return alice, bob

@pytest.fixture
def sample_message():
    """Provides a fully populated Message instance for serialization testing."""
    return Message(
        type=MessageType.RESULT,
        client_id="agent-007",
        cmd_id="cmd-999",
        command="whoami",
        result="root",
        exec_time_ms=45.2
    )