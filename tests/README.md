# C2 Project Tests

Comprehensive test suite for the C2 (Command & Control) project.

## Quick Start

### Run All Tests
```bash
# Using unittest
python run_tests.py

# Using pytest (if installed)
pytest tests/
```

### Run Specific Test Suite
```bash
# Unit tests only
python -m unittest discover tests/unit

# Integration tests only
python -m unittest discover tests/integration

# Specific test file
python -m unittest tests.unit.test_message
```

## Test Structure

```
tests/
├── unit/                           # Unit tests (isolated components)
│   ├── test_message.py            # Message protocol tests
│   ├── test_encryption.py         # Encryption & key management
│   ├── test_client_state.py      # Client lifecycle management
│   ├── test_client_manager.py    # Client registry
│   └── test_network.py            # Network communication
├── integration/                    # Integration tests (component interaction)
│   └── test_integration.py        # End-to-end flows
└── __init__.py
```

## Test Coverage

| Component | Coverage | Tests |
|-----------|----------|-------|
| Message Protocol | 100% | 10 |
| Encryption | 95% | 12 |
| Client State | 85% | 15 |
| Client Manager | 80% | 12 |
| Network | 90% | 7 |

## Key Test Files

### test_message.py
Tests message serialization, deserialization, and protocol correctness.
- JSON encoding/decoding
- Payload format with length prefix
- Factory methods for message types

### test_encryption.py
Tests ECDH key exchange and AES-256-GCM encryption.
- Key generation and serialization
- Session key derivation
- Encryption/decryption roundtrip
- Security validation

### test_client_state.py
Tests client lifecycle management.
- State transitions
- Command queue management
- Connection cleanup
- Encryption integration

### test_client_manager.py
Tests client registry and coordination.
- Client registration/lookup
- Command delegation
- Shutdown coordination

### test_network.py
Tests network message transmission.
- Send/receive with length prefix
- Error handling
- Connection failures

### test_integration.py
Tests end-to-end flows.
- Complete key exchange
- Multi-message encryption
- Registration flow
- Command/result flow

## Running with Coverage

```bash
# Install coverage tools
pip install pytest pytest-cov coverage

# Run with coverage report
pytest tests/ --cov=src --cov-report=html

# View coverage report
# Open htmlcov/index.html in browser
```

## Writing New Tests

### Unit Test Template
```python
import unittest
from unittest.mock import Mock, AsyncMock

class TestMyComponent(unittest.TestCase):
    def setUp(self):
        # Setup before each test
        pass
    
    def test_feature(self):
        # Arrange
        component = MyComponent()
        
        # Act
        result = component.do_something()
        
        # Assert
        self.assertEqual(result, expected)
```

### Async Test Template
```python
import unittest

class TestMyAsyncComponent(unittest.IsolatedAsyncioTestCase):
    async def test_async_feature(self):
        # Arrange
        component = MyAsyncComponent()
        
        # Act
        result = await component.do_something_async()
        
        # Assert
        self.assertEqual(result, expected)
```

## Mocking Guidelines

### Mock Network I/O
```python
from unittest.mock import AsyncMock, Mock

mock_reader = AsyncMock()
mock_writer = Mock()
mock_writer.write = Mock()
mock_writer.drain = AsyncMock()
```

### Mock Functions
```python
from unittest.mock import patch

@patch('module.function_name')
def test_with_mock(self, mock_func):
    mock_func.return_value = "mocked"
    # Test code
```

## Best Practices

1. **Isolation**: Each test should be independent
2. **Clarity**: Use descriptive test names
3. **Coverage**: Test happy paths and error cases
4. **Speed**: Mock slow operations
5. **Maintainability**: Keep tests simple and readable

## Troubleshooting

### Import Errors
Ensure you're running from project root:
```bash
cd c2_python
python -m unittest tests.unit.test_message
```

### Async Test Issues
Use `IsolatedAsyncioTestCase` for async tests:
```python
class TestAsync(unittest.IsolatedAsyncioTestCase):
    async def test_something(self):
        await async_function()
```

### Mock Not Working
- Use `AsyncMock` for async functions
- Use `Mock` for sync functions
- Verify patch path is correct

## CI/CD Integration

Tests should run automatically:
- Pre-commit hooks
- Pull request checks
- Deployment pipeline

## Documentation

See [TESTING.md](../TESTING.md) for comprehensive testing documentation.
