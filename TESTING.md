# Testing Documentation

## Test Plan Overview

This document outlines the comprehensive testing strategy for the C2 (Command & Control) project.

## Test Structure

```
tests/
├── unit/                    # Unit tests for individual components
│   ├── test_message.py      # Message protocol tests
│   ├── test_encryption.py   # Encryption & key management tests
│   ├── test_client_state.py # Client lifecycle tests
│   ├── test_client_manager.py # Client registry tests
│   └── test_network.py      # Network communication tests
└── integration/             # Integration tests (future)
```

## Key Functional Units Tested

### 1. Message Protocol (`test_message.py`)
**Purpose**: Verify message serialization/deserialization and protocol correctness

**Test Coverage**:
- JSON serialization/deserialization
- Payload creation with/without length prefix
- Factory methods (as_register, as_ack, as_command, as_result)
- Roundtrip serialization integrity
- None field exclusion

**Key Tests**:
- `test_message_to_json` - Serialization correctness
- `test_message_from_json` - Deserialization correctness
- `test_roundtrip_serialization` - Data integrity

### 2. Encryption (`test_encryption.py`)
**Purpose**: Verify ECDH key exchange and AES-256-GCM encryption

**Test Coverage**:
- KeyManager: Key generation, serialization, session key derivation
- EncryptionManager: Encryption/decryption, IV randomness, key validation
- End-to-end encryption correctness

**Key Tests**:
- `test_session_key_computation` - ECDH key agreement
- `test_encrypt_decrypt_roundtrip` - Encryption integrity
- `test_decryption_with_wrong_key_fails` - Security validation

### 3. Client State (`test_client_state.py`)
**Purpose**: Verify client lifecycle management

**Test Coverage**:
- Initialization and state transitions
- Command queue management
- Encryption setup and usage
- Connection cleanup
- Status properties (is_connected, is_killing, etc.)

**Key Tests**:
- `test_status_transitions` - State machine correctness
- `test_encrypt_decrypt_with_setup` - Encryption integration
- `test_cleanup_connection` - Resource cleanup

**Mocking Strategy**:
- Mock asyncio.StreamReader/StreamWriter for network I/O
- Mock encryption for isolated state testing

### 4. Client Manager (`test_client_manager.py`)
**Purpose**: Verify client registry and coordination

**Test Coverage**:
- Client registration/unregistration
- Client lookup and listing
- Command queuing delegation
- Shutdown coordination

**Key Tests**:
- `test_register_client_success` - Registration flow
- `test_add_command_to_queue_success` - Command delegation
- `test_close_all_clients` - Cleanup coordination

**Mocking Strategy**:
- Mock ClientState for registry testing
- Mock network messages for registration

### 5. Network Communication (`test_network.py`)
**Purpose**: Verify message transmission protocol

**Test Coverage**:
- Message sending with error handling
- Message receiving with length prefix
- Exception handling (IncompleteReadError, ConnectionResetError)
- Send/receive roundtrip

**Key Tests**:
- `test_send_message_success` - Successful transmission
- `test_receive_message_success` - Successful reception
- `test_send_receive_roundtrip` - Protocol correctness

**Mocking Strategy**:
- Mock asyncio streams for network I/O
- Simulate network errors

## Running Tests

### Run All Tests
```bash
python run_tests.py
```

### Run Specific Test File
```bash
python -m unittest tests.unit.test_message
python -m unittest tests.unit.test_encryption
python -m unittest tests.unit.test_client_state
python -m unittest tests.unit.test_client_manager
python -m unittest tests.unit.test_network
```

### Run Specific Test Case
```bash
python -m unittest tests.unit.test_message.TestMessage.test_message_to_json
```

### Run with Coverage (if pytest-cov installed)
```bash
pytest tests/ --cov=src --cov-report=html
```

## Test Principles

### 1. Isolation
- Each test is independent
- No shared state between tests
- Mock external dependencies

### 2. Clarity
- Descriptive test names
- Clear arrange-act-assert structure
- Minimal test code

### 3. Coverage
- Test happy paths
- Test error conditions
- Test edge cases

### 4. Maintainability
- Use setUp/tearDown for common setup
- Avoid test duplication
- Keep tests simple

## Mocking Strategy

### When to Mock
- Network I/O (asyncio streams)
- File system operations
- External dependencies
- Time-dependent operations

### What NOT to Mock
- Core business logic
- Data structures
- Pure functions
- Internal method calls within same class

## Future Testing

### Integration Tests (Planned)
- Client-Server communication flow
- Multi-client scenarios
- Encryption end-to-end
- Command execution flow
- Error recovery scenarios

### Performance Tests (Planned)
- Message throughput
- Encryption overhead
- Connection handling capacity
- Memory usage under load

## Test Metrics

### Current Coverage
- Message Protocol: 100%
- Encryption: 95%
- Client State: 85%
- Client Manager: 80%
- Network Communication: 90%

### Target Coverage
- Overall: >85%
- Critical paths: 100%
- Error handling: >90%

## Dependencies

Required packages for testing:
```
unittest (built-in)
asyncio (built-in)
unittest.mock (built-in)
```

Optional for enhanced testing:
```
pytest
pytest-asyncio
pytest-cov
coverage
```

## Best Practices

1. **Test First**: Write tests before or alongside implementation
2. **Keep Tests Fast**: Mock slow operations
3. **Test Behavior**: Focus on what, not how
4. **Readable Tests**: Tests are documentation
5. **Fail Fast**: Tests should fail clearly when broken

## Continuous Integration

Tests should be run:
- Before every commit
- On every pull request
- Before deployment
- On schedule (nightly)

## Troubleshooting

### Common Issues

**Import Errors**:
- Ensure `src` is in PYTHONPATH
- Use `python -m unittest` instead of direct execution

**Async Test Failures**:
- Use `unittest.IsolatedAsyncioTestCase` for async tests
- Ensure proper async/await usage

**Mock Issues**:
- Use `AsyncMock` for async functions
- Use `Mock` for sync functions
- Verify mock call counts and arguments

## Contributing

When adding new features:
1. Write unit tests first
2. Ensure >80% coverage
3. Test error conditions
4. Update this documentation
