import os
import json
import base64
from typing import Optional
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import x25519
from cryptography.hazmat.primitives import serialization

from models.protocol.message import Message


class EncryptionManager:
    """Handles key exchange and AES-256-GCM encryption/decryption"""
    
    def __init__(self):
        self._private_key = x25519.X25519PrivateKey.generate()
        self.session_key: Optional[bytes] = None
    
    @property
    def public_key_b64(self) -> str:
        """Get serialized public key as base64 string"""
        public_key_bytes = self._private_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw
        )
        return base64.b64encode(public_key_bytes).decode('ascii')
    
    def establish_session_key(self, peer_public_key_b64: str) -> None:
        """Compute session key from peer's public key"""
        peer_key_bytes = base64.b64decode(peer_public_key_b64)
        peer_public_key = x25519.X25519PublicKey.from_public_bytes(peer_key_bytes)
        
        shared_secret = self._private_key.exchange(peer_public_key)
        
        self.session_key = HKDF(
            algorithm=hashes.SHA256(),
            length=32,
            salt=None,
            info=b"payload-encryption-key",
            backend=default_backend()
        ).derive(shared_secret)
    
    def encrypt(self, message: Message, aad: Optional[str] = None) -> bytes:
        """Encrypt message with AES-256-GCM"""
        if not self.session_key:
            raise RuntimeError("Session key not established")
        
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
    
    def decrypt(self, encrypted_data: bytes, aad: Optional[str] = None) -> Optional[Message]:
        """Decrypt AES-256-GCM encrypted data"""
        if not self.session_key:
            raise RuntimeError("Session key not established")
        
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
