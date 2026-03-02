"""
Functional tests for complete client-server flows
Tests with real network connections and minimal mocking
"""
import pytest
import asyncio
import sys
from models.key_manager import KeyManager
from models.encryption_manager import EncryptionManager
from models.message import Message
from models.message_type import MessageType
from models.send_receive_msgs import send_message, receive_message


@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_real_network_message_exchange():
    """Test real TCP message exchange"""
    received_data = []
    
    async def handle_client(reader, writer):
        data = await receive_message(reader)
        received_data.append(data)
        msg = Message.from_payload(data)
        ack = Message.as_ack(msg.client_id, "server_key")
        await send_message(writer, ack.to_payload(with_prefix=True))
        writer.close()
        await writer.wait_closed()
    
    server = await asyncio.start_server(handle_client, '127.0.0.1', 0)
    port = server.sockets[0].getsockname()[1]
    
    try:
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        
        reg_msg = Message.as_register("test-client", "client_key")
        await send_message(writer, reg_msg.to_payload(with_prefix=True))
        
        ack_data = await receive_message(reader)
        ack = Message.from_payload(ack_data)
        
        assert ack.type == MessageType.ACK
        assert ack.client_id == "test-client"
        
        writer.close()
        await writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_encrypted_message_flow():
    """Test encrypted message exchange over real network"""
    server_em = EncryptionManager()
    
    async def handle_encrypted_client(reader, writer):
        data = await receive_message(reader)
        reg_msg = Message.from_payload(data)
        
        server_em.establish_session_key(reg_msg.command)
        
        ack = Message.as_ack(reg_msg.client_id, server_em.public_key_b64)
        await send_message(writer, ack.to_payload(with_prefix=True))
        
        cmd = Message.as_command("cmd-1", "whoami")
        encrypted = server_em.encrypt(cmd)
        await send_message(writer, encrypted)
        
        writer.close()
        await writer.wait_closed()
    
    server = await asyncio.start_server(handle_encrypted_client, '127.0.0.1', 0)
    port = server.sockets[0].getsockname()[1]
    
    try:
        client_em = EncryptionManager()
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        
        reg_msg = Message.as_register("client-1", client_em.public_key_b64)
        await send_message(writer, reg_msg.to_payload(with_prefix=True))
        
        ack_data = await receive_message(reader)
        ack = Message.from_payload(ack_data)
        client_em.establish_session_key(ack.command)
        
        encrypted_data = await receive_message(reader)
        decrypted = client_em.decrypt(encrypted_data)
        
        assert decrypted.type == MessageType.COMMAND
        assert decrypted.cmd_id == "cmd-1"
        assert decrypted.command == "whoami"
        
        writer.close()
        await writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_connection_drop_during_receive():
    """Test handling of connection drop during message receive"""
    async def handle_drop(reader, writer):
        await asyncio.sleep(0.1)
        writer.close()
        await writer.wait_closed()
    
    server = await asyncio.start_server(handle_drop, '127.0.0.1', 0)
    port = server.sockets[0].getsockname()[1]
    
    try:
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        
        with pytest.raises((asyncio.IncompleteReadError, ConnectionResetError)):
            await receive_message(reader)
        
        writer.close()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
@pytest.mark.timeout(15)
async def test_command_execution_end_to_end():
    """Test complete command execution flow with real subprocess"""
    from models.command_executor import CommandExecutor
    
    executor = CommandExecutor()
    in_queue = asyncio.Queue()
    out_queue = asyncio.Queue()
    
    exec_task = asyncio.create_task(executor.start(in_queue, out_queue))
    
    try:
        cmd = "echo functional_test" if sys.platform != "win32" else "cmd /c echo functional_test"
        await in_queue.put({"cmd_id": "func-1", "command": cmd})
        
        cmd_id, output = await asyncio.wait_for(out_queue.get(), timeout=5)
        
        assert cmd_id == "func-1"
        assert "functional_test" in output.lower()
    finally:
        executor.stop()
        exec_task.cancel()
        try:
            await exec_task
        except asyncio.CancelledError:
            pass
