"""
Key Management for ECDH key exchange and session key derivation
Separated from encryption operations for better SOLID compliance
"""

import base64
from typing import Optional
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import x25519
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend


class KeyManager:
    """Handles ECDH key exchange and session key derivation"""
    
    def __init__(self):
        self.private_key = x25519.X25519PrivateKey.generate()
        self.session_key: Optional[bytes] = None
    
    @property
    def public_key(self) -> x25519.X25519PublicKey:
        """Get the public key"""
        return self.private_key.public_key()
    
    @property
    def serialized_public_key(self) -> str:
        """Get the serialized public key as base64 string"""
        public_key_bytes = self.public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw
        )
        return base64.b64encode(public_key_bytes).decode('ascii')
    
    def compute_session_key(self, peer_public_key: str) -> bytes:
        """Compute session key from peer's public key"""
        peer_key_bytes = base64.b64decode(peer_public_key)
        peer_public_key_obj = x25519.X25519PublicKey.from_public_bytes(peer_key_bytes)
        
        shared_secret = self.private_key.exchange(peer_public_key_obj)
        
        self.session_key = HKDF(
            algorithm=hashes.SHA256(),
            length=32,
            salt=None,
            info=b"payload-encryption-key",
            backend=default_backend()
        ).derive(shared_secret)
        
        return self.session_key