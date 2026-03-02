"""
Helper utilities for testing
"""
import asyncio
from typing import Tuple
from unittest.mock import Mock


def create_mock_stream_pair() -> Tuple[asyncio.StreamReader, Mock]:
    """Create mock reader/writer pair for testing"""
    reader = asyncio.StreamReader()
    writer = Mock()
    writer.write = Mock()
    writer.drain = asyncio.coroutine(lambda: None)()
    writer.close = Mock()
    writer.wait_closed = asyncio.coroutine(lambda: None)()
    writer.is_closing = Mock(return_value=False)
    return reader, writer


async def wait_for_queue_item(queue: asyncio.Queue, timeout: float = 1.0):
    """Wait for item in queue with timeout"""
    return await asyncio.wait_for(queue.get(), timeout=timeout)


def assert_message_equal(msg1, msg2):
    """Assert two messages are equal"""
    assert msg1.type == msg2.type
    assert msg1.client_id == msg2.client_id
    assert msg1.cmd_id == msg2.cmd_id
    assert msg1.command == msg2.command
    assert msg1.result == msg2.result
