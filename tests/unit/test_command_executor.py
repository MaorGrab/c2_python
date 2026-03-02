"""
Unit tests for CommandExecutor
Tests with real subprocess execution
"""

import pytest
import asyncio
import sys
from models.command_executor import CommandExecutor


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_real_command_execution_echo(executor, queues):
    """Test actual command execution with echo"""
    in_queue, out_queue = queues
    
    cmd = "echo test" if sys.platform != "win32" else "cmd /c echo test"
    await in_queue.put({"cmd_id": "cmd-1", "command": cmd})
    
    executor_task = asyncio.create_task(executor.start(in_queue, out_queue))
    
    try:
        cmd_id, output, exec_time = await asyncio.wait_for(out_queue.get(), timeout=2.0)
        
        assert cmd_id == "cmd-1"
        assert "test" in output.lower()
        assert exec_time > 0
    finally:
        executor.stop()
        executor_task.cancel()
        try:
            await executor_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_real_command_execution_invalid(executor):
    """Test invalid command returns error message"""
    result = await executor._execute_command("cmd-1", "nonexistent_command_xyz_123")
    
    assert any(x in result for x in ["Error:", "not recognized", "not found"])


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_multiple_commands_sequential(executor, queues):
    """Test multiple commands execute sequentially"""
    in_queue, out_queue = queues
    
    cmd1 = "echo test1" if sys.platform != "win32" else "cmd /c echo test1"
    cmd2 = "echo test2" if sys.platform != "win32" else "cmd /c echo test2"
    
    await in_queue.put({"cmd_id": "cmd-1", "command": cmd1})
    await in_queue.put({"cmd_id": "cmd-2", "command": cmd2})
    
    executor_task = asyncio.create_task(executor.start(in_queue, out_queue))
    
    try:
        result1 = await asyncio.wait_for(out_queue.get(), timeout=2.0)
        result2 = await asyncio.wait_for(out_queue.get(), timeout=2.0)
        
        assert result1[0] == "cmd-1"
        assert result2[0] == "cmd-2"
        assert "test1" in result1[1].lower()
        assert "test2" in result2[1].lower()
    finally:
        executor.stop()
        executor_task.cancel()
        try:
            await executor_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_none_in_queue_continues_execution(executor, queues):
    """Test that None in queue doesn't break execution loop"""
    in_queue, out_queue = queues
    
    cmd = "echo test" if sys.platform != "win32" else "cmd /c echo test"
    
    await in_queue.put(None)
    await in_queue.put({"cmd_id": "cmd-1", "command": cmd})
    
    executor_task = asyncio.create_task(executor.start(in_queue, out_queue))
    
    try:
        result = await asyncio.wait_for(out_queue.get(), timeout=2.0)
        assert result[0] == "cmd-1"
    finally:
        executor.stop()
        executor_task.cancel()
        try:
            await executor_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_execution_process_cleared_after_success(executor):
    """Test that execution_process is cleared after successful execution"""
    cmd = "echo test" if sys.platform != "win32" else "cmd /c echo test"
    
    await executor._execute_command("cmd-1", cmd)
    
    assert executor.execution_process is None


@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_real_process_termination():
    """Test that long-running process is actually terminated"""
    executor = CommandExecutor()
    in_queue = asyncio.Queue()
    out_queue = asyncio.Queue()
    
    # Use long-running command
    cmd = "sleep 30" if sys.platform != "win32" else "timeout /t 30"
    await in_queue.put({"cmd_id": "cmd-1", "command": cmd})
    
    executor_task = asyncio.create_task(executor.start(in_queue, out_queue))
    
    # Let command start
    await asyncio.sleep(0.5)
    
    # Cancel executor
    executor.stop()
    executor_task.cancel()
    
    try:
        await asyncio.wait_for(executor_task, timeout=3.0)
    except asyncio.CancelledError:
        pass
    
    # Verify process was terminated
    assert executor.execution_process is None
