"""
Unit tests for encryption components
Tests KeyManager and EncryptionManager with real crypto
"""
import pytest
import base64
from models.key_manager import KeyManager
from models.encryption_manager import EncryptionManager
from models.message import Message


def test_key_generation():
    """Test KeyManager generates valid keys"""
    km = KeyManager()
    assert km.private_key is not None
    assert km.public_key is not None
    assert km.session_key is None


def test_serialized_public_key():
    """Test public key serialization"""
    km = KeyManager()
    serialized = km.serialized_public_key
    assert isinstance(serialized, str)
    decoded = base64.b64decode(serialized)
    assert len(decoded) == 32


def test_session_key_computation(key_manager_pair):
    """Test session key derivation"""
    km1, km2 = key_manager_pair
    assert km1.session_key == km2.session_key
    assert len(km1.session_key) == 32


def test_encryption_manager_initialization():
    """Test EncryptionManager initialization"""
    em = EncryptionManager()
    assert em.session_key is None
    assert em.public_key_b64 is not None


def test_encrypt_decrypt_roundtrip(encryption_pair):
    """Test complete encryption/decryption"""
    em1, em2 = encryption_pair
    original = Message.as_result("cmd-789", "test output", 100.5)
    
    encrypted = em1.encrypt(original)
    decrypted = em2.decrypt(encrypted[4:])
    
    assert decrypted.type == original.type
    assert decrypted.cmd_id == original.cmd_id
    assert decrypted.result == original.result
    assert decrypted.exec_time_ms == original.exec_time_ms


def test_encryption_produces_different_ciphertext(encryption_pair):
    """Test same message produces different ciphertext (random IV)"""
    em1, _ = encryption_pair
    msg = Message.as_command("cmd-001", "test")
    
    encrypted1 = em1.encrypt(msg)
    encrypted2 = em1.encrypt(msg)
    
    assert encrypted1 != encrypted2


def test_decryption_with_wrong_key_fails(encryption_pair):
    """Test decryption with wrong key fails"""
    em1, _ = encryption_pair
    msg = Message.as_command("cmd-999", "secret")
    encrypted = em1.encrypt(msg)
    
    em_wrong = EncryptionManager()
    em_wrong.session_key = b'0' * 32
    
    decrypted = em_wrong.decrypt(encrypted[4:])
    assert decrypted is None
