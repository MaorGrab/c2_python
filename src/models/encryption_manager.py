import os
import json
from typing import Dict
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

from models.message import Message


class EncryptionManager:
    """Handles AES-256-GCM encryption for STEP 2"""
    
    def __init__(self, master_key: bytes):
        """master_key: 32 bytes for AES-256"""
        assert len(master_key) == 32, "Master key must be 32 bytes (AES-256)"
        self.master_key = master_key
    
    def encrypt(self, message: Message, aad: str = None) -> bytes:
        """Encrypt message with AES-256-GCM"""
        iv = os.urandom(12)
        cipher = Cipher(
            algorithms.AES(self.master_key),
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
                algorithms.AES(self.master_key),
                modes.GCM(iv, tag),
                backend=default_backend()
            )
            decryptor = cipher.decryptor()
            
            if aad:
                decryptor.authenticate_additional_data(aad.encode())
            
            msg = decryptor.update(data) + decryptor.finalize()
            return Message.from_payload(msg)
        except Exception as e:
            # logger.error(f"Decryption failed: {e}")
            return None
