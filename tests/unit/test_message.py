import pytest
import json
from models.message import Message
from models.message_type import MessageType

# --- 1. FACTORY METHOD TESTS ---

@pytest.mark.parametrize("factory_func, kwargs, expected_type, expected_attrs", [
    (Message.as_register, {"client_id": "c1", "shared_key": "pub_key"}, MessageType.REGISTER, {"client_id": "c1", "command": "pub_key"}),
    (Message.as_ack, {"client_id": "c2", "peer_key": "srv_key"}, MessageType.ACK, {"client_id": "c2", "command": "srv_key"}),
    (Message.as_command, {"cmd_id": "cmd1", "command": "ls"}, MessageType.COMMAND, {"cmd_id": "cmd1", "command": "ls"}),
    (Message.as_result, {"cmd_id": "cmd2", "result": "ok", "exec_time_ms": 1.5}, MessageType.RESULT, {"cmd_id": "cmd2", "result": "ok", "exec_time_ms": 1.5}),
])
def test_message_factories(factory_func, kwargs, expected_type, expected_attrs):
    """Test all factory methods correctly map arguments to the dataclass."""
    msg = factory_func(**kwargs)
    
    assert msg.type == expected_type
    for attr, expected_val in expected_attrs.items():
        assert getattr(msg, attr) == expected_val


# --- 2. SERIALIZATION & ENCODING RULES ---

def test_to_json_strips_none_values():
    """Test that the serialization actively removes None values to save network bandwidth."""
    msg = Message(type=MessageType.COMMAND, cmd_id="123")
    json_str = msg.to_json()
    data = json.loads(json_str)
    
    assert "cmd_id" in data
    assert "client_id" not in data
    assert "result" not in data

def test_utf8_encoding_survival():
    """Test that multi-byte characters (e.g., emojis, foreign text) survive the payload roundtrip."""
    complex_text = "Result with emoji 🛑 and cyrillic привет"
    msg = Message(type=MessageType.RESULT, result=complex_text)
    
    payload = msg.to_payload()
    reconstructed = Message.from_payload(payload)
    
    assert reconstructed.result == complex_text

def test_payload_prefix_byte_math(sample_message):
    """Test the exact byte structure of the big-endian length prefix."""
    payload = sample_message.to_payload(with_prefix=True)
    
    # 1. Extract the first 4 bytes
    prefix_bytes = payload[:4]
    # 2. Extract the actual JSON message bytes
    message_bytes = payload[4:]
    
    # Assert big-endian conversion is mathematically correct
    expected_length = len(message_bytes)
    assert int.from_bytes(prefix_bytes, byteorder='big') == expected_length


# --- 3. DESERIALIZATION BOUNDARIES (EDGE CASES) ---

def test_from_payload_handles_empty_bytes():
    """Test that a silent network drop (empty bytes) safely returns None."""
    result = Message.from_payload(b"")
    assert result is None

def test_from_json_raises_on_invalid_json():
    """Test that malformed JSON strings bubble up standard decoding errors."""
    with pytest.raises(json.JSONDecodeError):
        Message.from_json("{this_is_not_json: true}")

def test_from_json_raises_on_invalid_enum():
    """Test that if an attacker sends an unknown MessageType, it is strictly rejected."""
    bad_json = json.dumps({"type": "UNKNOWN_HACKER_TYPE", "cmd_id": "1"})
    
    # Trying to cast a string not in the Enum raises ValueError
    with pytest.raises(ValueError):
        Message.from_json(bad_json)

def test_from_payload_raises_on_invalid_utf8():
    """Test that non-UTF-8 garbage bytes fail decoding immediately."""
    garbage_bytes = b"\xff\xfe\xfd"  # Invalid utf-8 sequence
    
    with pytest.raises(UnicodeDecodeError):
        Message.from_payload(garbage_bytes)

def test_from_json_raises_on_missing_type_key():
    """Test that valid JSON missing the mandatory 'type' field throws a KeyError."""
    # Valid JSON, but no "type" defined
    incomplete_json = json.dumps({"cmd_id": "123", "command": "whoami"})
    
    with pytest.raises(KeyError):
        Message.from_json(incomplete_json)

def test_from_json_raises_on_unexpected_fields():
    """Test that injecting unknown fields crashes the dataclass constructor."""
    # Valid type, valid syntax, but contains an extra unauthorized field
    rogue_json = json.dumps({
        "type": MessageType.COMMAND.value, 
        "cmd_id": "123", 
        "hacker_field": "bypassed"
    })
    
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        Message.from_json(rogue_json)