# MPMS Tests

This directory contains comprehensive test suites for MPMS functionality using pytest.

## Test Files

### test_mpms_basic.py
Core functionality tests using pytest:
- Initialization and configuration
- Worker and collector basic operations
- Error handling
- Meta class functionality
- Task queue behavior
- Concurrency tests

### test_mpms_lifecycle.py
Comprehensive lifecycle management tests using pytest:
- Count-based lifecycle (`lifecycle` parameter)
- Time-based lifecycle (`lifecycle_duration` parameter)
- Combined lifecycle scenarios
- Edge cases and error conditions
- Parametrized tests for various configurations

### test_mpms_performance.py
Performance and stress tests:
- High throughput testing
- Memory efficiency
- Scalability with different process/thread configurations
- Stress tests with rapid lifecycle rotation
- Concurrent operations stress testing
- Error recovery under load

## Installation

Install test dependencies:
```bash
pip install -r tests/requirements.txt
```

## Running Tests

### Using pytest directly:
```bash
# Run all tests
pytest tests/

# Run with verbose output
pytest tests/ -v

# Run with coverage report
pytest tests/ --cov=mpms --cov-report=html

# Run only quick tests (exclude slow/performance tests)
pytest tests/ -m "not slow"

# Run specific test file
pytest tests/test_mpms_basic.py

# Run tests in parallel (requires pytest-xdist)
pytest tests/ -n auto
```

### Using the test runner script:
```bash
# Run quick tests
python tests/run_tests.py --quick

# Run basic functionality tests
python tests/run_tests.py --basic

# Run lifecycle tests
python tests/run_tests.py --lifecycle

# Run performance tests
python tests/run_tests.py --performance

# Run all tests with coverage
python tests/run_tests.py --all --coverage -v
```

## Test Markers

Tests are marked with the following markers for easy filtering:
- `@pytest.mark.slow` - Tests that take a long time to run
- `@pytest.mark.performance` - Performance-related tests
- `@pytest.mark.stress` - Stress tests

## Coverage

To generate a coverage report:
```bash
pytest tests/ --cov=mpms --cov-report=html
# Open htmlcov/index.html in a browser
```

## Writing New Tests

When adding new tests:
1. Use pytest conventions (test_* functions in Test* classes)
2. Add appropriate markers for slow/performance tests
3. Use fixtures for common setup/teardown
4. Include docstrings explaining what each test does
5. Keep tests focused and independent 