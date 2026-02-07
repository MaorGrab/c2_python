"""
Unit tests for encryption components
Tests KeyManager and EncryptionManager functionality
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

import unittest
import base64
from models.key_manager import KeyManager
from models.encryption_manager import EncryptionManager
from models.message import Message
from models.message_type import MessageType


class TestKeyManager(unittest.TestCase):
    """Test KeyManager ECDH key exchange"""
    
    def test_key_generation(self):
        """Test that KeyManager generates valid keys"""
        km = KeyManager()
        
        self.assertIsNotNone(km.private_key)
        self.assertIsNotNone(km.public_key)
        self.assertIsNone(km.session_key)
    
    def test_serialized_public_key(self):
        """Test public key serialization"""
        km = KeyManager()
        serialized = km.serialized_public_key
        
        self.assertIsInstance(serialized, str)
        # Should be valid base64
        decoded = base64.b64decode(serialized)
        self.assertEqual(len(decoded), 32)  # X25519 public key is 32 bytes
    
    def test_session_key_computation(self):
        """Test session key derivation from peer public key"""
        km1 = KeyManager()
        km2 = KeyManager()
        
        # Exchange public keys
        pk1 = km1.serialized_public_key
        pk2 = km2.serialized_public_key
        
        # Compute session keys
        session_key1 = km1.compute_session_key(pk2)
        session_key2 = km2.compute_session_key(pk1)
        
        # Both should derive the same session key
        self.assertEqual(session_key1, session_key2)
        self.assertEqual(len(session_key1), 32)  # AES-256 key
    
    def test_session_key_stored(self):
        """Test that session key is stored after computation"""
        km1 = KeyManager()
        km2 = KeyManager()
        
        km1.compute_session_key(km2.serialized_public_key)
        
        self.assertIsNotNone(km1.session_key)
        self.assertEqual(len(km1.session_key), 32)


class TestEncryptionManager(unittest.TestCase):
    """Test EncryptionManager AES-256-GCM encryption"""
    
    def setUp(self):
        """Setup encryption managers with shared session key"""
        self.em1 = EncryptionManager()
        self.em2 = EncryptionManager()
        
        # Exchange keys and establish session
        pk1 = self.em1.public_key_b64
        pk2 = self.em2.public_key_b64
        
        self.em1.establish_session_key(pk2)
        self.em2.establish_session_key(pk1)
    
    def test_encryption_manager_initialization(self):
        """Test EncryptionManager initialization"""
        em = EncryptionManager()
        self.assertIsNone(em.session_key)
        self.assertIsNotNone(em.public_key_b64)
    
    def test_encrypt_message(self):
        """Test message encryption"""
        msg = Message.as_command("cmd-123", "whoami")
        encrypted = self.em1.encrypt(msg)
        
        self.assertIsInstance(encrypted, bytes)
        self.assertGreater(len(encrypted), 4)
    
    def test_decrypt_message(self):
        """Test message decryption"""
        original = Message.as_command("cmd-456", "ls -la")
        encrypted = self.em1.encrypt(original)
        
        encrypted_data = encrypted[4:]
        decrypted = self.em2.decrypt(encrypted_data)
        
        self.assertEqual(decrypted.type, original.type)
        self.assertEqual(decrypted.cmd_id, original.cmd_id)
        self.assertEqual(decrypted.command, original.command)
    
    def test_encrypt_decrypt_roundtrip(self):
        """Test complete encryption/decryption roundtrip"""
        original = Message.as_result("cmd-789", "test output", 100.5)
        
        encrypted = self.em1.encrypt(original)
        encrypted_data = encrypted[4:]
        decrypted = self.em2.decrypt(encrypted_data)
        
        self.assertEqual(decrypted.type, original.type)
        self.assertEqual(decrypted.cmd_id, original.cmd_id)
        self.assertEqual(decrypted.result, original.result)
        self.assertEqual(decrypted.exec_time_ms, original.exec_time_ms)
    
    def test_encryption_produces_different_ciphertext(self):
        """Test that same message produces different ciphertext (due to random IV)"""
        msg = Message.as_command("cmd-001", "test")
        
        encrypted1 = self.em1.encrypt(msg)
        encrypted2 = self.em1.encrypt(msg)
        
        self.assertNotEqual(encrypted1, encrypted2)
    
    def test_decryption_with_wrong_key_fails(self):
        """Test that decryption with wrong key fails"""
        msg = Message.as_command("cmd-999", "secret")
        encrypted = self.em1.encrypt(msg)
        
        # Create new encryption manager with different key
        em_wrong = EncryptionManager()
        em_wrong.session_key = b'0' * 32
        
        encrypted_data = encrypted[4:]
        decrypted = em_wrong.decrypt(encrypted_data)
        
        self.assertIsNone(decrypted)


if __name__ == '__main__':
    unittest.main()
