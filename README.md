# c2_python

Command & Control (C2) server and client implementation in Python.

## Quick Start

### 1. Clone and Setup
```bash
git clone <repo-url>
cd c2_python

# Create virtual environment
python -m venv .venv

# Activate (Windows)
.venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Install Pre-Push Hook (Recommended)
```bash
pre-commit install --hook-type pre-push
```

This runs tests automatically before pushing to remote.

### 3. Run Tests
```bash
python run_tests.py
```

## Project Structure

```
c2_python/
├── src/              # Source code
├── tests/            # Unit and integration tests
├── .pre-commit-config.yaml  # Pre-push hook configuration
└── .git/hooks/       # Installed hooks (auto-generated)
```

## Development

See [PRE_PUSH_HOOKS.md](PRE_PUSH_HOOKS.md) for git hook documentation.