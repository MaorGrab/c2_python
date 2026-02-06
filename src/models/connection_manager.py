import os
import logging
import asyncio

from models.tls_helper import TLSSessionHelper
from helper.auth import validate_server_certificate
from helper.helper_funcs import cancel_task

logger = logging.getLogger(__name__)


class ConnectionManager:
    def __init__(self, server_host: str, server_port: int, client_id: str):
        self.server_host = server_host
        self.server_port = server_port
        self.client_id = client_id
        self.reader = None
        self.writer = None
        self._connected = asyncio.Event()
        self._reconnect = asyncio.Event()
        self._reconnection_task = None
    
    @property
    def is_active(self):
        return self._reconnection_task is not None
    
    async def wait_connected(self):
        """Wait until connection is established"""
        if not self.is_active:
            return False
        await self._connected.wait()
        return True
        
    async def _open_connection(self) -> bool:
        """
        Connect to C2 server with certificate validation
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
            
            # Validate server certificate before marking as connected
            if not validate_server_certificate(self.writer):
                logger.error("Server certificate validation failed")
                return False
            
            logger.info(f"Connected to server at {self.server_host}:{self.server_port}")
            return True
        except ConnectionRefusedError:
            logger.info("Connection refused, server possibly down")
            return False
        except asyncio.CancelledError:
            logger.info("Connection attempt cancelled")
            return False
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False
    
    async def _reset_streams(self):
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

    async def trigger_reconnection(self):
        if not self.is_active:
            return
        self._set_events_reconnect()
        await self._reset_streams()

    def _set_events_reconnect(self):
        """
        Set reconnection events to allow reconnection loop to run
        """
        self._connected.clear()
        self._reconnect.set()

    def _set_events_connected(self):
        """
        Set events to indicate connection is established
        """
        self._connected.set()
        self._reconnect.clear()

    async def _reconnection_loop(self):
        """
        Background reconnection loop - runs continuously
        """
        try:
            reconnect_delay = 1
            
            while True:
                await self._reconnect.wait()
                logger.info(f"attempting to reconnect...")
                await self._reset_streams()
                
                if await self._open_connection():
                    logger.info("Reconnection successful")
                    self._set_events_connected()
                else:
                    await asyncio.sleep(reconnect_delay)
                    
        except asyncio.CancelledError:
            logger.info("Reconnection loop cancelled")
            raise
        except Exception as e:
            logger.error(f"Reconnection loop error: {e}")
            raise
        finally:
            await self._reset_streams()

    async def start(self):
        """Start connection manager with background reconnection"""
        if self.is_active:
            logger.warning("Connection manager already started")
            return
        self._set_events_reconnect()
        if await self._open_connection():
            self._set_events_connected()
        self._reconnection_task = asyncio.create_task(self._reconnection_loop(), name='reconnection')
        await self.wait_connected()  # Wait for initial connection

    async def shutdown(self):
        if not self.is_active:
            logger.warning("Trying to shutdown non-active connection manager")
        await cancel_task(self._reconnection_task)
        self._reconnection_task = None