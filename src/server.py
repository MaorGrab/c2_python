"""
C2 Server Implementation
Supports Steps 1-5 with modular design

Author: [Candidate]
References: 
- AsyncIO patterns: https://stackoverflow.com/questions/48506460/python-simple-socket-client-server-using-asyncio
- C2 Architecture: https://www.scip.ch/en/?labs.20250612
"""

import asyncio
import json
import logging
import time
import uuid
import argparse
from typing import Dict

# ==================== CONFIGURATION ====================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== CLIENT STATE MANAGEMENT ====================

class ClientState:
    """Tracks state for each connected client"""
    
    def __init__(self, client_id: str, reader, writer):
        self.client_id = client_id
        self.reader = reader
        self.writer = writer
        self.command_queue = asyncio.Queue()
        self.last_heartbeat = time.time()
        self.status = "connected"
        self.pending_results = {}  # {msg_id → result}

# ==================== MESSAGE HANDLING ====================

async def send_message(writer, message: Dict) -> bool:
    """
    Send JSON message to client
    Format: length_prefix(4 bytes)
    """
    try:        
        # Wrap data
        payload = json.dumps(message).encode()
        
        # Send with length prefix
        writer.write(len(payload).to_bytes(4, 'big') + payload)
        await writer.drain()
        return True
    except Exception as e:
        logger.error(f"Failed to send message: {e}")
        return False

async def receive_message(reader) -> Dict:
    """
    Receive JSON message from client
    """
    try:
        # Read length prefix
        length_data = await asyncio.wait_for(reader.readexactly(4), timeout=30)
        length = int.from_bytes(length_data, 'big')
        
        # Read payload
        payload = await asyncio.wait_for(reader.readexactly(length), timeout=30)
        plaintext = json.loads(payload.decode())
        
        return json.loads(plaintext) if plaintext else None
    except asyncio.TimeoutError:
        logger.warning("Message receive timeout")
        return None
    except Exception as e:
        logger.error(f"Failed to receive message: {e}")
        return None

# ==================== CORE C2 LOGIC ====================

class C2Server:
    """Main C2 Server"""
    
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.clients: Dict[str, ClientState] = {}
        self.running = True
    
    async def handle_client(self, reader, writer):
        """
        Implements command recv/exec/result loop
        """
        client_id = None
        client_state = None
        
        try:
            # Receive registration message
            msg = await receive_message(reader)
            if not msg or msg.get("type") != "register":
                logger.warning("Invalid registration message")
                writer.close()
                return
            
            client_id = msg.get("client_id", f"client-{uuid.uuid4().hex[:8]}")
            client_state = ClientState(client_id, reader, writer)
            
            # Register client
            self.clients[client_id] = client_state
            logger.info(f"Client registered: {client_id}")
            
            # Send acknowledgment
            await send_message(writer, {"type": "ack", "client_id": client_id})
            
            # Main client loop
            await self._client_loop(client_state)
            
        except Exception as e:
            logger.error(f"Client handler error: {e}")
        finally:
            if client_id and client_id in self.clients:
                logger.info(f"Client disconnected: {client_id}")
            if client_state:
                client_state.writer.close()
    
    async def _client_loop(self, state: ClientState):
        """
        Main loop for client communication
        """
        command_receiver = asyncio.create_task(self._command_receiver(state))
        command_executor = asyncio.create_task(self._command_executor(state))
        
        try:
            await asyncio.gather(
                command_receiver,
                command_executor,
            )
        except asyncio.CancelledError:
            pass
        finally:
            command_receiver.cancel()
            command_executor.cancel()

    async def _command_receiver(self, state: ClientState):
        """
        Receive messages from client
        """
        while self.running:
            try:
                if state.status != "connected":
                    logger.info(f"Client {state.client_id} not connected, stopping receiver")
                    break
                msg = await receive_message(state.reader)
                if not msg:
                    break
                
                msg_type = msg.get("type")

                if msg_type == "result":
                    cmd_id = msg.get("cmd_id")
                    result = msg.get("result", "")
                    exec_time = msg.get("exec_time_ms", 0)
                    
                    logger.info(f"Result from {state.client_id}: {result[:100]}")
                    
                    if cmd_id in state.pending_results:
                        del state.pending_results[cmd_id]
                
                else:
                    logger.warning(f"Unknown message type: {msg_type}")
            
            except Exception as e:
                logger.error(f"Command receiver error: {e}")
                break
    
    async def _command_executor(self, state: ClientState):
        """
        STEP 4: Execute commands queued for this client
        Uses asyncio.Queue for serialization
        """
        while self.running:
            try:
                if state.status != "connected":
                    logger.info(f"Client {state.client_id} not connected, stopping executor")
                    break
                # Get next command from queue (timeout prevents hanging)
                cmd_data = await asyncio.wait_for(state.command_queue.get(), timeout=1.0)
                
                cmd_id = cmd_data.get("cmd_id")
                command = cmd_data.get("command")
                
                # Track pending result
                state.pending_results[cmd_id] = command
                
                # Send command to client
                await send_message(
                    state.writer,
                    {
                        "type": "command",
                        "cmd_id": cmd_id,
                        "command": command
                    },
                )
                logger.info(f"Command sent to {state.client_id}: {command}")
            
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Command executor error: {e}")
                break

    async def start_server(self):
        """Start the C2 server"""
        server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port
        )
        
        addr = server.sockets[0].getsockname()
        logger.info(f"C2 Server listening on {addr[0]}:{addr[1]}")
        
        async with server:
            try:
                await server.serve_forever()
            except KeyboardInterrupt:
                logger.info("Server shutdown")
                self.running = False
    
    # ==================== ADMIN CLI (STEP 1) ====================
    
    def cmd_list_clients(self):
        """List all connected clients"""
        if not self.clients:
            print("No clients connected")
            return
        
        print("\n" + "="*60)
        print(f"{'Client ID':<20} {'Status':<12} {'Last HB':<12}")
        print("="*60)
        
        for client_id, state in self.clients.items():
            last_hb = time.time() - state.last_heartbeat
            hb_str = f"{last_hb:.1f}s ago"
            print(f"{client_id:<20} {state.status:<12} {hb_str:<12}")
        
        print("="*60 + "\n")
    
    def cmd_run(self, args: str):
        """
        Run command on client
        Usage: run <client_id> <command>
        """
        parts = args.split(None, 1)
        if len(parts) < 2:
            print("Usage: run <client_id> <command>")
            return
        
        client_id, command = parts
        
        if client_id not in self.clients:
            print(f"Client {client_id} not found")
            return
        client_status = self.clients[client_id].status
        if client_status == "killed":
            print(f"Client {client_id} was killed - not connected")
            return
        elif client_status != "connected":
            print(f"Client {client_id} not in connected state ({client_status})")
            return
        
        # Queue command
        cmd_id = str(uuid.uuid4())
        self.clients[client_id].command_queue.put_nowait({
            "cmd_id": cmd_id,
            "command": command
        })
        
        print(f"Command queued for {client_id}: {command}")
    
    def cmd_kill(self, client_id: str):
        """Kill client connection"""
        if client_id not in self.clients:
            print(f"Client {client_id} not found")
            return
        
        # Send kill command first
        cmd_id = str(uuid.uuid4())
        client_state = self.clients[client_id]
        client_state.command_queue.put_nowait({
            "cmd_id": cmd_id,
            "command": "kill"
        })
        
        print(f"Kill command sent to {client_id}")
        client_state.status = "killed"
    
    async def admin_cli(self):
        """
        Admin CLI loop
        Runs in separate thread/coroutine
        """
        loop = asyncio.get_event_loop()
        
        while self.running:
            try:
                # Run input in executor to avoid blocking
                cmd = await loop.run_in_executor(None, input, "> ")
                
                if cmd.startswith("list"):
                    self.cmd_list_clients()
                
                elif cmd.startswith("run"):
                    self.cmd_run(cmd[4:].strip())
                
                elif cmd.startswith("kill"):
                    self.cmd_kill(cmd[5:].strip())
                
                elif cmd.startswith("exit"):
                    self.running = False
                    break
                
                elif cmd == "help":
                    print("""
                Commands:
                list              - List connected clients
                run <id> <cmd>    - Run command on client
                kill <id>         - Kill client
                exit              - Exit server
                    """)
                
                else:
                    print("Unknown command. Type 'help'")
            
            except EOFError:
                break
            except Exception as e:
                logger.error(f"CLI error: {e}")

# ==================== MAIN ====================

async def main():
    parser = argparse.ArgumentParser(description="C2 Server")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    parser.add_argument("--port", type=int, default=5000, help="Bind port")

    args = parser.parse_args()
    server = C2Server(args.host, args.port)
    
    # Run server and CLI concurrently
    await asyncio.gather(
        server.start_server(),
        server.admin_cli()
    )

if __name__ == "__main__":
    asyncio.run(main())