import os
import json
from typing import Dict, Optional
import base64
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import x25519
from cryptography.hazmat.primitives import serialization

from models.message import Message


class EncryptionManager:
    """Handles AES-256-GCM encryption for STEP 2"""
    # TODO: check if master key was assigned
    # TODO: make the session key generation more general and with better function names
    
    def __init__(self, master_key: bytes = None):
        self.session_key: Optional[bytes] = master_key
        self.private_key = x25519.X25519PrivateKey.generate()

    @property
    def public_key(self) -> bytes:
        """Get the public key of the encryption manager"""
        return self.private_key.public_key()
    
    @property
    def _serialized_public_key_bytes(self) -> bytes:
        """Get the serialized public key of the encryption manager"""
        return self.public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw
        )
    
    @property
    def serialized_public_key(self) -> str:
        """Get the serialized public key of the encryption manager as a string"""
        return base64.b64encode(self._serialized_public_key_bytes).decode('ascii')
    
    @staticmethod
    def _load_public_key(serialized_public_key: str) -> bytes:
        """Load a public key from a string"""
        return base64.b64decode(serialized_public_key)
    
    def compute_session_key(self, peer_public_key: str) -> None:
        """Compute the shared secret and derive the session key"""
        peer_public_key_bytes = self._load_public_key(peer_public_key)
        shared_secret = self._compute_shared_secret(peer_public_key_bytes)
        self._derive_session_key(shared_secret)

    def _compute_shared_secret(self, peer_public_key: bytes) -> bytes:
        """Compute the ECDH shared secret using the peer's public key."""
        peer_public_key = x25519.X25519PublicKey.from_public_bytes(peer_public_key)
        return self.private_key.exchange(peer_public_key)

    def _derive_session_key(self, shared_secret: bytes) -> None:
        """Derive a symmetric session key from the ECDH shared secret using HKDF."""
        self.session_key = HKDF(
            algorithm=hashes.SHA256(),
            length=32,
            salt=None,
            info=b"payload-encryption-key",
            backend=default_backend()
        ).derive(shared_secret)
    
    def encrypt(self, message: Message, aad: str = None) -> bytes:
        """Encrypt message with AES-256-GCM"""
        iv = os.urandom(12)
        cipher = Cipher(
            algorithms.AES(self.session_key),
            modes.GCM(iv),
            backend=default_backend()
        )
        encryptor = cipher.encryptor()
        
        if aad:
            encryptor.authenticate_additional_data(aad.encode())
        
        data = encryptor.update(message.to_payload()) + encryptor.finalize()
        aes_dict = {
            "iv": iv.hex(),
            "data": data.hex(),
            "tag": encryptor.tag.hex()
        }
        data_to_send = json.dumps(aes_dict).encode()
        prefix = len(data_to_send).to_bytes(4, 'big')
        return prefix + data_to_send
    
    def decrypt(self, encrypted_data: bytes, aad: str = None) -> Message:
        """Decrypt AES-256-GCM encrypted data"""
        try:
            aes_dict = json.loads(encrypted_data.decode())
            iv = bytes.fromhex(aes_dict["iv"])
            data = bytes.fromhex(aes_dict["data"])
            tag = bytes.fromhex(aes_dict["tag"])
            
            cipher = Cipher(
                algorithms.AES(self.session_key),
                modes.GCM(iv, tag),
                backend=default_backend()
            )
            decryptor = cipher.decryptor()
            
            if aad:
                decryptor.authenticate_additional_data(aad.encode())
            
            msg = decryptor.update(data) + decryptor.finalize()
            return Message.from_payload(msg)
        except Exception as e:
            print(f"Decryption failed: {e}")
            return None
