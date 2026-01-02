import subprocess
import asyncio
import signal
import sys
import os


SUBPROCESS_TERMINATION_GRACE_TIME_S = 2


def _is_windows() -> bool:
    return 'win' in sys.platform

def _generate_subprocess_flags() -> dict:
    if _is_windows():
        return {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
    else:
        return {"start_new_session": True}

async def create_subprocess(command: str) -> asyncio.subprocess.Process:
    execution_process = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        **_generate_subprocess_flags()
    )
    return execution_process

async def communicate_subprocess(process: asyncio.subprocess.Process) -> str:
    stdout, stderr = await process.communicate()
    output = stdout.decode() + (f"\n[stderr] {stderr.decode()}" if stderr else "")
    return output if output else "[No output]"

async def terminate_subprocess_gracefully(process: asyncio.subprocess.Process):
    """Terminate a subprocess gracefully"""
    if _is_windows():
        process.send_signal(signal.CTRL_BREAK_EVENT)
    else:
        os.killpg(process.pid, signal.SIGTERM)
    await asyncio.wait_for(
        process.wait(),
        timeout=SUBPROCESS_TERMINATION_GRACE_TIME_S
    )

async def terminate_subprocess_forcefully(process: asyncio.subprocess.Process):
    """Terminate a subprocess forcefully"""
    process.kill()
    await process.wait()
    