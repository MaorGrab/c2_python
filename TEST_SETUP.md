# Test Setup Instructions

## Install Dependencies

Before running tests, install required dependencies:

```bash
pip install -r requirements.txt
```

## Run Tests

```bash
# Run all tests
python run_tests.py

# Run specific test file
python -m unittest tests.unit.test_message
python -m unittest tests.unit.test_encryption
python -m unittest tests.unit.test_client_state
python -m unittest tests.unit.test_client_manager
python -m unittest tests.unit.test_network
```

## Import Fix Applied

All test files now correctly add the `src` directory to `sys.path` before importing modules:

```python
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

# Now imports work correctly
from models.message import Message
```

This allows the tests to import from `models.*` which matches the relative imports used in the source code.
