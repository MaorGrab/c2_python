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
        self._running = True
        
    async def _open_connection(self) -> bool:
        """
        Connect to C2 server and register
        """
        try:
            if not self._running:
                return False
            ssl_ctx = TLSSessionHelper().create_context(
                is_server=False,
            )
            self.reader, self.writer = await asyncio.open_connection(
                self.server_host,
                self.server_port,
                ssl=ssl_ctx,
            )
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

    async def reconnection_loop(self):
        """
        Auto-reconnect logic
        Attempts to reconnect every 1 second if disconnected
        """
        try:
            reconnect_delay = 1
        
            await self._clear_connection()
            while self._running:
                if self.reader and self.writer:
                    logger.info('reader/writer exists - no reconnection needed')
                    return
                
                if await self._open_connection():
                    return
                
                logger.info(f"Attempting to reconnect in {reconnect_delay}s...")
                await asyncio.sleep(reconnect_delay)
            
        except asyncio.CancelledError:
            logger.info("Reconnect loop cancelled")
            await self._clear_connection()
            raise
        except Exception as e:
            logger.error(f"Reconnect loop error: {e}")
            await self._clear_connection()
            raise

    async def close(self):
        self._running = False
        await self._clear_connection()

    @property
    def is_connected(self):
        return self.reader is not None and self.writer is not None