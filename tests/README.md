# C2 Project Tests

Modern pytest-based test suite with real operations and minimal mocking.

## Quick Start

### Run All Tests
```bash
# Using pytest (recommended)
python run_tests.py

# Or directly
pytest tests/

# With coverage
pytest tests/ --cov=src --cov-report=html

# Parallel execution
pytest tests/ -n auto
```

### Run Specific Test Suite
```bash
# Unit tests
pytest tests/unit/ -v

# Integration tests
pytest tests/integration/ -v

# Functional tests (real I/O)
pytest tests/functional/ -v

# Performance tests
pytest tests/performance/ -v -m performance

# Specific file
pytest tests/unit/test_message.py -v
```

## Test Structure

```
tests/
├── unit/                    # Unit tests (minimal mocking)
├── integration/             # Integration tests
├── functional/              # Real I/O operations
├── performance/             # Load and stress tests
├── fixtures/                # Shared test utilities
│   ├── __init__.py
│   └── test_helpers.py
└── conftest.py              # Pytest fixtures
```

## Test Philosophy

### Minimal Mocking
- Unit tests use real operations where possible
- Functional tests use real network/subprocess
- Mocks only for external dependencies

### Real Operations
- Actual subprocess execution
- Real TCP connections
- Real encryption operations

### Performance Testing
- Concurrent client handling
- Command queue stress
- Memory leak detection

## Key Test Files

### Unit Tests
- `test_message.py` - Message protocol (no mocks)
- `test_encryption.py` - Real crypto operations
- `test_command_executor.py` - Real subprocess execution
- `test_network.py` - Network send/receive

### Functional Tests
- `test_client_server_flow.py` - Real network operations
- End-to-end encrypted communication
- Connection failure handling

### Performance Tests
- `test_load.py` - Concurrent clients, stress testing
- Marked with `@pytest.mark.performance`

## Running with Coverage

```bash
# HTML report
pytest tests/ --cov=src --cov-report=html

# Terminal report
pytest tests/ --cov=src --cov-report=term-missing

# Minimum coverage threshold
pytest tests/ --cov=src --cov-fail-under=80
```

## Pytest Features Used

### Fixtures
- Reusable test setup in `conftest.py`
- `encryption_pair`, `key_manager_pair`, etc.

### Markers
- `@pytest.mark.asyncio` - Async tests
- `@pytest.mark.timeout(N)` - Timeout protection
- `@pytest.mark.performance` - Performance tests

### Parametrization
```python
@pytest.mark.parametrize("input,expected", [
    ("test1", "result1"),
    ("test2", "result2"),
])
def test_multiple_cases(input, expected):
    assert process(input) == expected
```

## Parallel Execution

```bash
# Run tests in parallel
pytest tests/ -n auto

# Specify worker count
pytest tests/ -n 4
```

## CI/CD Integration

Tests run automatically:
- Pre-commit hooks
- Pull request checks
- Deployment pipeline

## Troubleshooting

### Import Errors
Ensure running from project root:
```bash
cd c2_python
pytest tests/
```

### Async Test Issues
Use `@pytest.mark.asyncio` decorator:
```python
@pytest.mark.asyncio
async def test_something():
    await async_function()
```

### Timeout Issues
Increase timeout for slow tests:
```python
@pytest.mark.timeout(30)
async def test_slow_operation():
    # ...
```

## Best Practices

1. **Isolation**: Each test should be independent
2. **Clarity**: Use descriptive test names
3. **Coverage**: Test happy paths and error cases
4. **Speed**: Mock only when necessary
5. **Maintainability**: Keep tests simple and readable
