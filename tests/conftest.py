"""
Pytest configuration with shared fixtures for all tests
"""

import sys
import asyncio
from pathlib import Path
import pytest

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


@pytest.fixture
async def command_executor():
    """Provide CommandExecutor instance"""
    from models.command_executor import CommandExecutor
    executor = CommandExecutor()
    yield executor
    executor.stop()


@pytest.fixture
def client_manager():
    """Provide ClientManager instance"""
    from models.client_manager import ClientManager
    return ClientManager()
