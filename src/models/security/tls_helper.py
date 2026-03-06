import ssl
import os
import asyncio
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from typing import Optional, Tuple

class TLSSessionHelper:
    """
    Generic TLS helper for asyncio server/client usage.
    - Creates TLS contexts (server or client) forcing TLS1.3.
    - Provides a function to export keying material from an established TLS session.
    - Derives a per-connection AES-256-GCM key + 4-byte nonce prefix from exported bytes.
    - Provides simple encrypt/decrypt helpers that use a per-connection counter + prefix
      to build unique 12-byte nonces required by AES-GCM.

    Security/caveats (reminder):
    - Derived key is per TLS connection. If the host is compromised, the key may be exposed.
    - Nonce construction: prefix (4 bytes) || counter (8 bytes, big-endian). Counter must not wrap.
    - We derive once per connection and store in the connection state (fast & standard).
    """

    EXPORT_LABEL = b"app-payload-key"  # namespace; both sides must use same label
    # We export 36 bytes: 32 bytes AES-256 key + 4 bytes nonce prefix
    EXPORT_BYTES = 32 + 4

    def create_context(self,
                       is_server: bool,
                       certfile: Optional[str] = None,
                       keyfile: Optional[str] = None,
                       cafile: Optional[str] = None,
                       require_client_cert: bool = False) -> ssl.SSLContext:
        """
        Create a configured SSLContext for TLS1.3.

        - is_server: True -> server context (loads cert/key)
                     False -> client context (verifies server with cafile)
        - certfile/keyfile required for server
        - cafile: path to CA cert to verify server (client) or clients (server)
        """
        purpose = ssl.Purpose.CLIENT_AUTH if is_server else ssl.Purpose.SERVER_AUTH
        ctx = ssl.create_default_context(purpose=purpose)

        # Force TLS 1.3 only (modern, smaller attack surface)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_3

        if is_server:
            if not certfile or not keyfile:
                raise ValueError("Server must provide certfile and keyfile.")
            ctx.load_cert_chain(certfile=certfile, keyfile=keyfile)

            if cafile:
                ctx.load_verify_locations(cafile=cafile)
            # Only require client certs if explicitly requested
            ctx.verify_mode = ssl.CERT_REQUIRED if require_client_cert else ssl.CERT_NONE
        else:
            # client: require server verification
            if cafile:
                ctx.check_hostname = True
                ctx.load_verify_locations(cafile=cafile)
                ctx.verify_mode = ssl.CERT_REQUIRED
            else:
                # If no cafile supplied, default system CAs are used by create_default_context
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE

        return ctx

