import asyncio
import logging
import time
import helper.subprocess as subprocess_helper

logger = logging.getLogger(__name__)


class CommandExecutor:
    """Handles command execution and result reporting"""
    
    def __init__(self):
        self.execution_process = None
        self._should_stop = False
    
    async def start(self, in_qeueue: asyncio.Queue, out_queue: asyncio.Queue):
        """Start executor loop"""
        try:
            while not self._should_stop:
                # Receive command
                command_data = await in_qeueue.get()
                if command_data is None:
                    logger.warning("Received None command")
                    continue
                if self._should_stop:
                    break
                cmd_id = command_data.get("cmd_id")
                command = command_data.get("command", "").lower()
                
                # Execute command
                cmd_id, result, exec_time_ms = await self._process_command((cmd_id, command))
                
                # Send result
                await out_queue.put((cmd_id, result, exec_time_ms))
                
        except asyncio.CancelledError:
            logger.info('Executor cancelled')
            raise
        except Exception as e:
            logger.error(f"Executor error: {e}")
            raise
        finally:
            logger.info("Executor finished")
    
    async def _process_command(self, command_data: tuple[str, str]) -> tuple[str, str, float]:
        """Process a single command"""
        cmd_id, command = command_data
        start_time = time.monotonic()
        result = await self._execute_command(cmd_id, command)
        exec_time_ms = (time.monotonic() - start_time) * 1000
        logger.info(f"Executed: {command} ({exec_time_ms:.1f}ms)")
        return cmd_id, result, exec_time_ms
    
    async def _execute_command(self, cmd_id: str, command: str) -> str:
        """Execute bash-style command"""
        try:
            self.execution_process = await subprocess_helper.create_subprocess(command)
            logger.info(f"[cmd id: {cmd_id}] Execution process created (pid: {self.execution_process.pid})")
            output = await subprocess_helper.communicate_subprocess(self.execution_process)
            self.execution_process = None
            return output
        except asyncio.CancelledError:
            await self._terminate_execution_process()
            raise
        except Exception as e:
            logger.error(f"Command execution error: {str(e)}")
            return f"Error: {str(e)}"
    
    async def _terminate_execution_process(self):
        """Terminate execution process if it exists"""
        if not self.execution_process:
            return
        pid = self.execution_process.pid
        try:
            await subprocess_helper.terminate_subprocess_gracefully(self.execution_process)
            self.execution_process = None
            logger.info(f"Terminated execution process gracefully (pid: {pid})")
        except asyncio.TimeoutError:
            logger.error("Graceful termination timed out")
        except Exception as e:
            logger.error(f"Error gracefully terminating process: {e}")
        finally:
            if not self.execution_process:
                return
            try:
                await subprocess_helper.terminate_subprocess_forcefully(self.execution_process)
                self.execution_process = None
                logger.info(f"Terminated execution process forcefully (pid: {pid})")
            except Exception as e:
                logger.error(f"Error forcefully terminating process: {e}")
    
    def stop(self):
        """Stop executor"""
        self._should_stop = True
    
    @property
    def is_killed(self) -> bool:
        """Check if KILL command was executed"""
        return self._should_stop
