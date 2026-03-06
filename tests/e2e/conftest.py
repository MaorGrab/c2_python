import pytest_asyncio
import asyncio
from client import C2Client
from server import C2Server
from c2_python.src.models.client_state import ClientState
from c2_python.tests.utils import poll_until


@pytest_asyncio.fixture
async def make_server():
    servers = []  # Keep track for emergency cleanup

    async def _create_server(port: int = 0):
        server = C2Server("127.0.0.1", port)
        # We store the task on the object so we can find it later
        server._test_task = asyncio.create_task(server.start_server())

        while not server._server or not server._server.sockets:
            await asyncio.sleep(0.01)
        
        servers.append(server)
        return server

    yield _create_server

    # MANUAL TEARDOWN for all servers created by the factory
    for s in servers:
        s.shutdown.set()
        s._test_task.cancel()
        try:
            await s._test_task
        except asyncio.CancelledError:
            pass
    await asyncio.sleep(0.1)

@pytest_asyncio.fixture
async def make_client():
    clients = []

    async def _create_client(port: int, id_: str = 'test-agent'):
        client = C2Client("127.0.0.1", port, id_)
        client._test_task = asyncio.create_task(client.start())
        clients.append(client)
        return client

    yield _create_client

    for c in clients:
        c._test_task.cancel()
        try:
            await c._test_task
        except asyncio.CancelledError:
            pass
    await asyncio.sleep(0.1)

@pytest_asyncio.fixture
async def c2_env(make_server, make_client):
    server = await make_server(port=0)
    assigned_port = server._server.sockets[0].getsockname()[1]
    client = await make_client(port=assigned_port)

    is_connected = await poll_until(
        lambda: client.client_id in server.client_manager.clients,
        timeout=3
    )
    assert is_connected, "Client failed to connect"
    
    yield server, client

@pytest_asyncio.fixture
def captured_results(mocker):
    """
    Fixture that intercepts ClientState._handle_result to silently capture 
    incoming result strings into a list, without breaking production behavior.
    """
    captured = []
    original_handle_result = ClientState._handle_result
    
    async def capture_wrapper(self_instance, msg):
        captured.append(str(msg.result))  # Steal a copy for the test
        await original_handle_result(self_instance, msg)  # Execute original behavior
        
    # Apply the patch 
    mocker.patch("models.client_manager.ClientState._handle_result", capture_wrapper)
    
    # Return the live list to the test
    return captured