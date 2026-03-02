"""
Performance tests for C2 system
Tests load handling and concurrent operations
"""
import pytest
import asyncio
import sys
from models.message import Message
from models.send_receive_msgs import send_message, receive_message


@pytest.mark.performance
@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_concurrent_client_connections():
    """Test server handles multiple concurrent clients"""
    num_clients = 20
    connected_clients = []
    
    async def handle_client(reader, writer):
        try:
            data = await receive_message(reader)
            msg = Message.from_payload(data)
            ack = Message.as_ack(msg.client_id, "server_key")
            await send_message(writer, ack.to_payload(with_prefix=True))
            connected_clients.append(msg.client_id)
        finally:
            writer.close()
            await writer.wait_closed()
    
    server = await asyncio.start_server(handle_client, '127.0.0.1', 0)
    port = server.sockets[0].getsockname()[1]
    
    async def client_session(client_id):
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        reg = Message.as_register(client_id, "key")
        await send_message(writer, reg.to_payload(with_prefix=True))
        ack_data = await receive_message(reader)
        writer.close()
        await writer.wait_closed()
        return client_id
    
    try:
        tasks = [client_session(f"client-{i}") for i in range(num_clients)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        failures = [r for r in results if isinstance(r, Exception)]
        assert len(failures) == 0, f"{len(failures)}/{num_clients} clients failed"
        assert len(connected_clients) == num_clients
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.performance
@pytest.mark.asyncio
@pytest.mark.timeout(20)
async def test_rapid_message_exchange():
    """Test rapid message sending/receiving"""
    message_count = 100
    received = []
    
    async def handle_messages(reader, writer):
        for i in range(message_count):
            data = await receive_message(reader)
            received.append(data)
            ack = Message.as_ack(f"ack-{i}", "ok")
            await send_message(writer, ack.to_payload(with_prefix=True))
        writer.close()
        await writer.wait_closed()
    
    server = await asyncio.start_server(handle_messages, '127.0.0.1', 0)
    port = server.sockets[0].getsockname()[1]
    
    try:
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        
        for i in range(message_count):
            msg = Message.as_command(f"cmd-{i}", f"test{i}")
            await send_message(writer, msg.to_payload(with_prefix=True))
            await receive_message(reader)
        
        writer.close()
        await writer.wait_closed()
        
        assert len(received) == message_count
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.performance
@pytest.mark.asyncio
@pytest.mark.timeout(15)
async def test_command_queue_stress():
    """Test command queue handles many queued commands"""
    from models.command_executor import CommandExecutor
    
    executor = CommandExecutor()
    in_queue = asyncio.Queue()
    out_queue = asyncio.Queue()
    
    num_commands = 10
    cmd = "echo stress" if sys.platform != "win32" else "cmd /c echo stress"
    
    exec_task = asyncio.create_task(executor.start(in_queue, out_queue))
    
    try:
        for i in range(num_commands):
            await in_queue.put({"cmd_id": f"stress-{i}", "command": cmd})
        
        results = []
        for _ in range(num_commands):
            result = await asyncio.wait_for(out_queue.get(), timeout=10)
            results.append(result)
        
        assert len(results) == num_commands
        assert all("stress" in r[1].lower() for r in results)
    finally:
        executor.stop()
        exec_task.cancel()
        try:
            await exec_task
        except asyncio.CancelledError:
            pass
