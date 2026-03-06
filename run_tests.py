"""
Test runner for C2 project
Runs pytest test suite with coverage
"""

import sys
import subprocess


def run_tests():
    """Run pytest with coverage"""
    cmd = [
        sys.executable, '-m', 'pytest',
        'tests/',
        '-v',
        '--cov=src',
        '--cov-report=term-missing',
        '--cov-report=html',
        '-m', 'not performance',
        '--tb=short'
    ]
    
    result = subprocess.run(cmd)
    return result.returncode


def run_all_tests():
    """Run all tests including performance"""
    cmd = [sys.executable, '-m', 'pytest', 'tests/', '-v', '--cov=src']
    result = subprocess.run(cmd)
    return result.returncode


if __name__ == '__main__':
    sys.exit(run_tests())
