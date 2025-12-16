"""
C2 Server Implementation
Supports Steps 1-5 with modular design

Author: [Candidate]
References: 
- AsyncIO patterns: https://stackoverflow.com/questions/48506460/python-simple-socket-client-server-using-asyncio
- C2 Architecture: https://www.scip.ch/en/?labs.20250612
"""

import asyncio
import logging
import time
import os
import uuid
import argparse
import base64
from typing import Dict, List
from models.message import Message
from models.send_receive_msgs import send_message, receive_message
from models.client_state import ClientState
from models.message_type import MessageType
from models.command_type import CommandType
from models.encryption_manager import EncryptionManager

# ==================== CONFIGURATION ====================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== CORE C2 LOGIC ====================

class C2Server:
    """Main C2 Server"""
    
    def __init__(self, host: str, port: int, master_key: bytes):
        self.host = host
        self.port = port
        self.clients: Dict[str, ClientState] = {}
        self.shutdown = asyncio.Event()   # <-- shared shutdown signal
        self._server: asyncio.base_events.Server | None = None
        self._encryption_manager = EncryptionManager(master_key)
    
    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """
        Implements command recv/exec/result loop
        """
        try:
            msg = await receive_message(reader)  # Receive registration message
            msg = Message.from_payload(msg)
            if not msg or msg.type is not MessageType.REGISTER:
                logger.warning(f"Invalid registration message: {msg.type} of type {type(msg.type)}")
                writer.close()
                return
            client_id = msg.client_id
            client_state = ClientState(client_id, reader, writer)
            self.clients[client_id] = client_state
            logger.info(f"Client registered: {client_id}")
            # Send acknowledgment
            master_key = base64.b64encode(self._encryption_manager.master_key).decode()
            await send_message(writer, Message.as_ack(client_id, master_key).to_payload(True))
            # Main client loop
            await self._client_loop(client_state)
        except asyncio.CancelledError:
            logger.info("Client handler cancelled")
        except Exception as e:
            logger.error(f"Client handler error: {e}")

    async def _client_loop(self, client_state: ClientState):
        """
        Main loop for client communication
        """
        try:
            tasks = [
                asyncio.create_task(self._command_receiver(client_state)),
                asyncio.create_task(self._command_executor(client_state)),
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            logger.info("Client loop cancelled")
        finally:
            await self._cancel_tasks(tasks)
            
    @staticmethod
    async def _cancel_tasks(tasks: List[asyncio.Task]):
        try:
            for task in tasks:
                if not task.done():
                    task.cancel()
        except Exception as e:
            logger.error(f"Error cancelling tasks: {e}")

    async def _command_receiver(self, client_state: ClientState):
        """
        Receive messages from client
        """
        try:
            while True:
                if not client_state.is_connected:
                    await self._handle_killed_client(client_state)
                    break
                msg = await receive_message(client_state.reader)
                if not msg:
                    break
                if self.shutdown.is_set():
                    break
                msg = self._encryption_manager.decrypt(msg)
                if msg.type is MessageType.RESULT:
                    logger.info(f"Result from {client_state.client_id}: {msg.result[:100]}")
                    if msg.cmd_id in client_state.pending_results:
                        del client_state.pending_results[msg.cmd_id]   
                        if client_state.is_killing:
                            if client_state.pending_results:
                                continue  # wait for all results to arrive
                            else:
                                client_state.set_killed()
                else:
                    logger.warning(f"Unknown message type: {msg.type}")
            
        except asyncio.CancelledError:
            logger.info("Command receiver cancelled")
        except Exception as e:
            logger.error(f"Command receiver error: {e}")
    
    async def _command_executor(self, client_state: ClientState):
        """
        Execute commands queued for this client
        Uses asyncio.Queue for serialization
        """
        try:
            while True:
                if not client_state.is_connected:
                    break
                cmd_data = await client_state.command_queue.get()
                if self.shutdown.is_set():
                    break
                cmd_id = cmd_data.get("cmd_id")
                command = cmd_data.get("command")
                
                client_state.pending_results[cmd_id] = command
                msg = Message.as_command(cmd_id, command)
                msg = self._encryption_manager.encrypt(msg)
                await send_message(
                    client_state.writer,
                    msg
                )
                logger.info(f"Command sent to {client_state.client_id}: {command}")
            
        except asyncio.CancelledError:
            logger.info("Command executor cancelled")
        except Exception as e:
            logger.error(f"Command executor error: {e}")

    async def _handle_killed_client(self, client_state: ClientState) -> None:
        """Handle killed client connection"""
        if not client_state.is_killed:
            return
        if not client_state.writer or client_state.writer.is_closing():
            return
        client_state.writer.close()
        try:
            await asyncio.wait_for(client_state.writer.wait_closed(), timeout=1.0)
        except asyncio.TimeoutError:
            logger.info(f"Timeout waiting for writer to close reached for {client_state.client_id}")
        client_state.writer = None
        client_state.reader = None
        logger.info(f"Closed writer for killed client {client_state.client_id}")

    async def start_server(self):
        """Start the C2 server"""
        self._server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port
        )
        addr = self._server.sockets[0].getsockname()
        logger.info(f"C2 Server listening on {addr[0]}:{addr[1]}")
        async with self._server:
            try:
                logger.info("Server started")
                await self.shutdown.wait()
                logger.info("Server received shutdown event")
            except KeyboardInterrupt:
                logger.info("Server received Keyboard Interrupt event")
            except asyncio.CancelledError:
                logger.info("Server cancelled")
            except Exception as e:
                logger.error(f"Server error: {e}")
            finally:
                await self.stop()
                logger.info("Server stopped")

    async def stop(self):
        if not self._server:
            logger.info("Server not running, for some reason")
            return
        # Stop accepting new connections first
        logger.info("Server closing...")
        self._server.close()
        # Then close existing clients
        await self.close_writers()
        await self._server.wait_closed()

    async def close_writers(self) -> None:
        """Close all client writers"""
        for client_id, client_state in self.clients.items():
            if client_state.writer and not client_state.writer.is_closing():
                client_state.writer.close()
                try:
                    await asyncio.wait_for(client_state.writer.wait_closed(), timeout=1.0)
                except asyncio.TimeoutError:
                    logger.info(f"Timeout waiting for writer to close reached for {client_id}")
                logger.info(f"Closed writer for {client_id}")
        self.clients.clear()
    
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
            logger.info("Command too short. Usage: run <client_id> <command>")
            return
        
        client_id, command = parts
        
        if client_id not in self.clients:
            logger.info(f"Client {client_id} not found")
            return
        client = self.clients[client_id]
        if client.is_killed:
            logger.info(f"Client {client_id} was killed - not connected")
            return
        elif not client.is_connected:
            logger.info(f"Client {client_id} not in connected state ({client})")
            return
        self._add_command_to_queue(client_id, command)
        logger.info(f"Command queued for {client_id}: {command}")
    
    def cmd_kill(self, client_id: str):
        """Kill client connection"""
        if client_id not in self.clients:
            logger.info(f"Client {client_id} not found")
            return
        self._add_command_to_queue(client_id, CommandType.KILL.value)
        logger.info(f"Kill command sent to {client_id}")
        client_state = self.clients[client_id]
        client_state.set_killing()

    def _add_command_to_queue(self, client_id: str, command: str) -> None:
        cmd_id = str(uuid.uuid4())
        client_state = self.clients[client_id]
        client_state.command_queue.put_nowait({
            "cmd_id": cmd_id,
            "command": command,
        })
    
    async def admin_cli(self):
        """
        Admin CLI loop
        Runs in separate thread/coroutine
        """
        loop = asyncio.get_event_loop()
        
        try:
            while not self.shutdown.is_set():
                if self.shutdown.is_set():
                    break
                cmd = await loop.run_in_executor(None, input, "> ")
                if not cmd:
                    continue
                cmd_parts = cmd.lower().split(maxsplit=1)
                cmd_type = CommandType(cmd_parts[0])
                
                if cmd_type is CommandType.LIST:
                    self.cmd_list_clients()
                
                elif cmd_type is CommandType.EXIT:
                    logger.info('Exiting server')
                    self.shutdown.set()
                
                elif cmd_type is CommandType.RUN:
                    self.cmd_run(cmd_parts[1].strip())
                
                elif cmd_type is CommandType.KILL:
                    self.cmd_kill(cmd_parts[1].strip())
                
                elif cmd_type is CommandType.HELP:
                    print("""
Commands:
list              - List connected clients
run <id> <cmd>    - Run command on client
kill <id>         - Kill client
exit              - Exit server
                    """)
                
                else:
                    logger.info(f"Unknown command {cmd}. Type 'help'")
            
        except asyncio.CancelledError:
            logger.info("CLI cancelled")
        except KeyboardInterrupt:
            logger.info("CLI received Keyboard Interrupt event")
        except Exception as e:
            logger.error(f"CLI error: {e}")

# ==================== MAIN ====================

async def main():
    parser = argparse.ArgumentParser(description="C2 Server")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    parser.add_argument("--port", type=int, default=5000, help="Bind port")
    parser.add_argument("--secret-key", help="Master key (32 hex chars for AES-256)")

    args = parser.parse_args()
    # Generate or parse key
    if args.secret_key:
        try:
            master_key = bytes.fromhex(args.secret_key)
            if len(master_key) != 32:
                raise ValueError("Key must be 32 bytes")
        except:
            print("Error: --secret-key must be 64 hex characters (32 bytes)")
            return
    else:
        master_key = os.urandom(32)
        print(f"Generated master key: {master_key.hex()}")
        print("Use --secret-key to specify this for clients\n")

    server = C2Server(args.host, args.port, master_key)
    
    # Run server and CLI concurrently
    await asyncio.gather(
        server.start_server(),
        server.admin_cli()
    )

if __name__ == "__main__":
    asyncio.run(main())