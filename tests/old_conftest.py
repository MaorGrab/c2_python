"""
Pytest configuration with shared fixtures for all tests
"""

import sys
import asyncio
from pathlib import Path
import pytest
import pytest_asyncio
from unittest.mock import Mock, AsyncMock

# Add src directory to Python path for imports
src_path = Path(__file__).parent.parent / 'src'
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def encryption_pair():
    """Provide two encryption managers with established session"""
    from models.encryption_manager import EncryptionManager
    em1 = EncryptionManager()
    em2 = EncryptionManager()
    em1.establish_session_key(em2.public_key_b64)
    em2.establish_session_key(em1.public_key_b64)
    return em1, em2


@pytest.fixture
def key_manager_pair():
    """Provide two key managers with computed session keys"""
    from models.key_manager import KeyManager
    km1 = KeyManager()
    km2 = KeyManager()
    km1.compute_session_key(km2.serialized_public_key)
    km2.compute_session_key(km1.serialized_public_key)
    return km1, km2


@pytest_asyncio.fixture
async def executor():
    """Provide CommandExecutor instance with cleanup"""
    from models.command_executor import CommandExecutor
    exec_instance = CommandExecutor()
    yield exec_instance
    exec_instance.stop()


@pytest_asyncio.fixture
async def queues():
    """Provide input and output queues"""
    return asyncio.Queue(), asyncio.Queue()


@pytest.fixture
def cm():
    """Provide CommunicationManager instance"""
    from models.communication_manager import CommunicationManager
    return CommunicationManager("127.0.0.1", 5000, "test-client")


@pytest.fixture
def peer_em():
    """Provide peer EncryptionManager for testing"""
    from models.encryption_manager import EncryptionManager
    return EncryptionManager()


@pytest.fixture
def connection_manager():
    """Provide ConnectionManager instance"""
    from models.connection_manager import ConnectionManager
    return ConnectionManager("127.0.0.1", 5000, "test-client")


@pytest.fixture
def mock_reader_writer():
    """Provide mock reader/writer pair for I/O boundary testing"""
    mock_reader = AsyncMock()
    mock_writer = Mock()
    mock_writer.write = Mock()
    mock_writer.drain = AsyncMock()
    mock_writer.is_closing = Mock(return_value=False)
    mock_writer.close = Mock()
    mock_writer.wait_closed = AsyncMock()
    return mock_reader, mock_writer


@pytest.fixture
def client_manager():
    """Provide ClientManager instance"""
    from models.client_manager import ClientManager
    return ClientManager()
