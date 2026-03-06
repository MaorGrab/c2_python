import pytest
import json
import base64
import binascii
from c2_python.src.models.message import Message
from c2_python.src.models.command_type import CommandType

# --- 1. STATE & HANDSHAKE TESTS ---

def test_public_key_generation_and_format(encryption_manager):
    """Test that the public key is generated immediately and is valid Base64."""
    pub_key = encryption_manager.public_key_b64
    
    assert isinstance(pub_key, str)
    # Validate it is properly base64 encoded
    decoded = base64.b64decode(pub_key)
    # X25519 public keys are exactly 32 bytes
    assert len(decoded) == 32

def test_session_key_establishment_creates_identical_keys(em_pair):
    """Test the Elliptic-Curve Diffie-Hellman (ECDH) key exchange works symmetrically."""
    alice, bob = em_pair
    
    # Both parties must independently derive the exact same 32-byte AES key
    assert alice.session_key is not None
    assert bob.session_key is not None
    assert alice.session_key == bob.session_key
    assert len(alice.session_key) == 32

def test_encryption_methods_raise_if_uninitialized(encryption_manager):
    """Test defensive mechanism preventing encryption without a session key."""
    dummy_msg = Message.as_command("cmd-1", "whoami")
    
    with pytest.raises(RuntimeError, match="Session key not established"):
        encryption_manager.encrypt(dummy_msg)
        
    with pytest.raises(RuntimeError, match="Session key not established"):
        encryption_manager.decrypt(b"some_data")


# --- 2. ENCRYPTION & DECRYPTION ROUNDTRIP ---

def test_encrypt_decrypt_happy_path(em_pair):
    """Test that Alice can encrypt and Bob can decrypt the exact message."""
    alice, bob = em_pair
    original_msg = Message.as_command("cmd-123", CommandType.KILL.value)
    
    # Alice encrypts
    encrypted_payload = alice.encrypt(original_msg)
    
    # The payload MUST have a 4-byte length prefix natively
    assert len(encrypted_payload) > 4
    
    # Bob decrypts (stripping the 4-byte prefix, which the network reader normally does)
    json_payload = encrypted_payload[4:]
    decrypted_msg = bob.decrypt(json_payload)
    
    assert decrypted_msg is not None
    assert decrypted_msg.cmd_id == original_msg.cmd_id
    assert decrypted_msg.command == original_msg.command
    assert decrypted_msg.type == original_msg.type

def test_encryption_produces_unique_ciphertexts(em_pair):
    """Test that identical messages produce different ciphertexts (proving IVs are random)."""
    alice, _ = em_pair
    msg = Message.as_command("cmd-1", "ipconfig")
    
    payload1 = alice.encrypt(msg)
    payload2 = alice.encrypt(msg)
    
    # If this fails, the AES-GCM nonce/IV is static, which is a catastrophic security flaw.
    assert payload1 != payload2


# --- 3. CRYPTOGRAPHIC BOUNDARIES & TAMPERING ---

def test_decrypt_fails_on_tampered_ciphertext(em_pair):
    """Test that AES-GCM catches data tampering and safely returns None."""
    alice, bob = em_pair
    msg = Message.as_command("cmd-1", "ls")
    
    encrypted_payload = alice.encrypt(msg)
    json_payload = encrypted_payload[4:]
    
    # Arrange: Tamper with the JSON payload
    # We load the JSON, alter one character of the ciphertext, and re-encode
    aes_dict = json.loads(json_payload.decode())
    
    # Flip the last hex character of the encrypted data
    tampered_data = list(aes_dict["data"])
    tampered_data[-1] = '0' if tampered_data[-1] != '0' else '1'
    aes_dict["data"] = "".join(tampered_data)
    
    tampered_payload = json.dumps(aes_dict).encode()
    
    # Act
    result = bob.decrypt(tampered_payload)
    
    # Assert: The GCM tag validation must fail, triggering the exception block
    assert result is None

def test_aad_validation(em_pair):
    """Test that Additional Authenticated Data (AAD) is strictly enforced."""
    alice, bob = em_pair
    msg = Message.as_command("cmd-1", "whoami")
    secret_aad = "session-id-4455"
    wrong_aad = "session-id-9999"
    
    # Encrypt with AAD
    encrypted_payload = alice.encrypt(msg, aad=secret_aad)
    json_payload = encrypted_payload[4:]
    
    # Act 1: Decrypt with NO AAD
    assert bob.decrypt(json_payload) is None
    
    # Act 2: Decrypt with WRONG AAD
    assert bob.decrypt(json_payload, aad=wrong_aad) is None
    
    # Act 3: Decrypt with CORRECT AAD
    assert bob.decrypt(json_payload, aad=secret_aad) is not None

def test_decrypt_handles_malformed_json(em_pair):
    """Test that completely invalid bytes don't crash the manager."""
    _, bob = em_pair
    
    # Act: Feed it garbage bytes instead of a JSON payload
    result = bob.decrypt(b"this is not json")
    
    # Assert: Safely absorbed by the generic Exception block
    assert result is None

def test_establish_session_key_raises_on_invalid_base64(encryption_manager):
    """Test that strictly non-Base64 input triggers a binascii.Error."""
    with pytest.raises(binascii.Error):
        encryption_manager.establish_session_key("this-is-not-base64-!@#$")

def test_establish_session_key_raises_on_wrong_key_length(encryption_manager):
    """Test that valid Base64 with incorrect byte length triggers a ValueError."""
    # Arrange: 16 bytes instead of the required 32 for X25519
    wrong_length_bytes = base64.b64encode(b"0" * 16).decode('ascii')
    
    # Assert: Must explicitly catch the cryptography library's length validation error
    with pytest.raises(ValueError):
        encryption_manager.establish_session_key(wrong_length_bytes)

def test_decrypt_handles_missing_json_keys(em_pair):
    """Test that valid JSON missing required cryptographic keys is safely caught."""
    _, bob = em_pair
    
    # Arrange: Missing the "iv" key
    incomplete_payload = json.dumps({"data": "00", "tag": "00"}).encode()
    
    # Act & Assert
    assert bob.decrypt(incomplete_payload) is None


def test_decrypt_handles_invalid_hex_strings(em_pair):
    """Test that non-hexadecimal strings in the JSON payload are safely caught."""
    _, bob = em_pair
    
    # Arrange: "zzzz" is not valid hex and will crash bytes.fromhex()
    bad_hex_payload = json.dumps({"iv": "zzzz", "data": "00", "tag": "00"}).encode()
    
    # Act & Assert
    assert bob.decrypt(bad_hex_payload) is None