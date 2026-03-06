import pytest
import asyncio

from c2_python.tests.utils import poll_until


@pytest.mark.asyncio
async def test_full_command_roundtrip_in_process(c2_env):
    server, client = c2_env

    # 1. Issue a command directly via the Server's method
    # This bypasses the need for the CLI/stdin
    server.cmd_run(f"{client.client_id} echo INTEGRATION_SUCCESS")

    # 2. Poll for the result arrival in the server's internal state
    # We check if the pending_results for that specific client is cleared
    # (Or you could check the result_queue in the client's communication manager)
    client_state = server.client_manager.get_client(client_id=client.client_id)
    
    def result_received():
        # In your code, _handle_result deletes from pending_results
        # So we check if the dict is empty after being populated
        return len(client_state.pending_results) == 0

    assert await poll_until(result_received), "Server never received command result"


@pytest.mark.asyncio
async def test_kill_logic_in_process(c2_env, mocker):
    server, client = c2_env

    # 1. Attach the Spy
    # We spy on 'stop' to ensure the cleanup logic was triggered
    spy_executor_stop = mocker.spy(client.executor, 'stop') 
    
    # 2. Issue the Kill command from the Server
    server.cmd_kill(client.client_id)

    # 3. POLL FOR HARD STATE: Is the task actually finished?
    # We check if the internal asyncio task is done. 
    # This is the most reliable way to know the client has stopped.
    client_finished = await poll_until(
        lambda: client._executor_task.done(),
        timeout=5.0
    )

    assert client_finished, "Client executor task never reached 'done' state"

    # 4. ASSERT: Validation of the logic path
    # Even if is_killed wasn't set, we verify the stop method was invoked 
    # as part of the teardown.
    spy_executor_stop.assert_called_once()

@pytest.mark.asyncio
async def test_mid_execution_kill_aborts_process(c2_env):
    """Test that a kill command instantly aborts a running OS process."""
    server, client = c2_env
        
    # 1. Issue a long-running OS command (10 seconds)
    long_cmd = 'python -c "import time; time.sleep(10)"'
    server.cmd_run(f"{client.client_id} {long_cmd}")
    
    # Give the executor a fraction of a second to actually start the subprocess
    await asyncio.sleep(0.5)
    
    # 2. Issue the Kill command while the subprocess is still running
    server.cmd_kill(client.client_id)
    
    # 3. Poll for the client to die
    client_finished = await poll_until(lambda: client._executor_task.done(), timeout=5.0)
    assert client_finished, "Client did not abort the long-running process to shut down"
    
    # 4. Prove the command queue didn't block the kill command
    assert client.executor.is_killed is True

@pytest.mark.asyncio
async def test_offline_result_queued_and_delivered_on_reconnect(c2_env, make_server, captured_results, mocker):
    """Test that results generated while offline are saved and sent upon reconnection."""
    server, client = c2_env
    port = server.port

    # 1. Issue a command
    desired_result = "offline success"
    sleep_cmd = f'python -c "import time; time.sleep(0.2); print(\\"{desired_result}\\")"'
    server.cmd_run(f"{client.client_id} {sleep_cmd}")
    await asyncio.sleep(0.1) 
    
    assert len(captured_results) == 0, f"captures result before server shutdown {captured_results}"
    # 2. Crash the server immediately (Client is now orphaned)
    server.shutdown.set()
    await server.stop()
    
    # 3. Wait until the command finishes while client is offline.
    await asyncio.sleep(0.2)
    
    # 4. Bring a NEW server back online on the EXACT SAME PORT
    new_server = await make_server(port=port)
    
    try:
        # 5. Wait for the orphaned client to successfully reconnect
        reconnected = await poll_until(
            lambda: client.client_id in new_server.client_manager.clients, 
            timeout=1.0
        )
        assert reconnected, "Client failed to reconnect"
        
        # 6. ASSERT: Did the interceptor catch the ghost payload?
        # We just check our local list!
        received = await poll_until(
            lambda: any(desired_result in res for res in captured_results), 
            timeout=1.0
        )
        assert received, f"Expected {desired_result}, but captured: {captured_results}"
        
        # 7. Final check: Client queue should now be empty
        drained = await poll_until(lambda: client.communication_manager.result_queue.empty())
        assert drained, "Client did not drain its offline queue"
        
    finally:
        new_server.shutdown.set()

@pytest.mark.asyncio
async def test_invalid_os_command_handled_gracefully(c2_env):
    """Test that garbage OS commands return an error result but keep the client alive."""
    server, client = c2_env
    await poll_until(lambda: client.client_id in server.client_manager.clients)
    
    # 1. Send a command that definitely does not exist
    server.cmd_run(f"{client.client_id} definitely_not_a_real_binary_12345")
    
    # 2. Wait for the server to process the result
    client_state = server.client_manager.get_client(client.client_id)
    
    def result_processed():
        return len(client_state.pending_results) == 0
        
    assert await poll_until(result_processed), "Server never received the error result"
    
    # 3. Assert the client is still perfectly healthy and its task is not done
    assert not client._executor_task.done(), "Client crashed after executing an invalid command!"

# @pytest.mark.asyncio
# async def test_concurrent_command_bombardment(c2_env):
#     """Test that rapid-fire commands are queued and routed back with perfectly matching IDs."""
#     server, client = c2_env
#     await poll_until(lambda: client.client_id in server.client_manager.clients)
#     client_state = server.client_manager.get_client(client.client_id)
    
#     # 1. Fire 3 commands back-to-back instantly without waiting
#     command = "python -c 'import time; time.sleep(1);'"
#     for _ in range(3):
#         server.cmd_run(f"{client.client_id} {command}")
#     await asyncio.sleep(0)  # give brief control to server
#     # 2. Verify all 3 were immediately registered in the server's tracking dictionary
#     assert len(client_state.pending_results) == 3, f"Server failed to track concurrent commands {len(client_state.pending_results)}"
    
#     # 3. Poll until the queue is completely drained and all 3 results are returned
#     drained_successfully = await poll_until(
#         lambda: len(client_state.pending_results) == 0, 
#         timeout=10.0
#     )
    
#     assert drained_successfully, "Client failed to process and return all concurrent commands"

@pytest.mark.asyncio
async def test_rogue_socket_isolation(c2_env):
    """Test that garbage TCP connections do not crash the server for valid clients."""
    server, valid_client = c2_env
    await poll_until(lambda: valid_client.client_id in server.client_manager.clients)
        
    # 1. Act as a port scanner: Open a raw socket and send garbage
    reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
    writer.write(b"NMAP_SCAN_GARBAGE_BYTES_!!@#$")
    await writer.drain()
    
    # Close the rogue socket abruptly
    writer.close()
    await writer.wait_closed()
    
    # 2. Give the server a moment to throw and catch the internal exception
    await asyncio.sleep(0)
    
    # 3. Assert the valid client is still perfectly alive and can receive commands
    server.cmd_run(f"{valid_client.client_id} echo ISOLATION_SUCCESS")
    
    client_state = server.client_manager.get_client(valid_client.client_id)
    assert await poll_until(lambda: len(client_state.pending_results) == 0, timeout=5.0), \
        "The rogue socket crashed the server's event loop or message router!"

@pytest.mark.asyncio
async def test_duplicate_client_id_reconnect_overwrite(c2_env, make_client, captured_results):
    """Test that a new client connecting with an existing ID cleanly hijacks the session."""
    server, client_1 = c2_env
    
    # 1. Wait for Client 1 to be fully registered
    await poll_until(lambda: client_1.client_id in server.client_manager.clients)
    
    # 2. Start Client 2 with the EXACT SAME ID
    # make_client handles the asyncio task creation and guaranteed teardown!
    client_2 = await make_client(server.port)
    
    # Give the server a moment to accept the new TCP socket and overwrite the registry
    await asyncio.sleep(0.5)
    
    # 3. Sabotage Client 1
    # We locally kill Client 1. If the server incorrectly routes the next command 
    # to Client 1's old socket, it drops into the void and the test fails.
    client_1.executor.stop()
    
    # 4. Issue a command to the shared client ID
    desired_result = "hijack_success"
    server.cmd_run(f"{client_1.client_id} echo {desired_result}")
    
    # 5. ASSERT: Did the interceptor catch the payload from Client 2?
    # Because Client 1 is dead, ONLY Client 2 can possibly return this result.
    received = await poll_until(
        lambda: any(desired_result in res for res in captured_results), 
        timeout=2.0
    )
    
    assert received, f"Expected '{desired_result}', but captured: {captured_results}"
