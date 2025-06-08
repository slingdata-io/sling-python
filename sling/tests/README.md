# Sling Python Package Tests

This directory contains comprehensive tests for the Sling Python package to ensure basic functionality works correctly.

## Test Structure

The test suite includes:

### Core Class Tests
- **TestMode**: Tests the Mode enum values (FULL_REFRESH, INCREMENTAL, etc.)
- **TestSource**: Tests the Source class initialization and options handling
- **TestTarget**: Tests the Target class with various option configurations
- **TestTaskOptions**: Tests the TaskOptions class
- **TestTask**: Tests the deprecated Task class for backward compatibility

### Replication Tests
- **test_replication_stream**: Tests ReplicationStream class with hooks, enable/disable functionality
- **test_replication**: Tests the main Replication class with stream management
- **test_replication_with_file_path**: Tests file-based replication configurations
- **test_replication_json_serialization**: Tests JSON serialization of replication configs

### Pipeline Tests
- **test_pipeline**: Tests Pipeline class with steps and environment variables
- **test_pipeline_with_file_path**: Tests file-based pipeline configurations

### Execution Tests (Mocked)
- **TestMockedExecution**: Tests execution methods with mocked binary calls
  - Replication run success/failure scenarios
  - Pipeline run success/failure scenarios
  - Task run success/failure scenarios
  - CLI function testing
  - Exception handling

### Environment and Error Handling
- **TestEnvironmentHandling**: Tests environment variable merging
- **TestErrorHandling**: Tests various error scenarios and edge cases
  - Invalid mode handling
  - Empty configuration handling
  - Temp file cleanup on success/error

### Integration Tests (Optional)
- **TestRealBinaryExecution**: Tests that require the actual sling binary
  - Version command testing
  - Help command testing
  - These tests are skipped if `SLING_BINARY` environment variable is not set

## Running the Tests

### Using pytest (Recommended)

```bash
# Install pytest if not already installed
pip install pytest pytest-mock

# Run all tests
python -m pytest tests/tests.py -v

# Run specific test classes
python -m pytest tests/tests.py::TestMode -v
python -m pytest tests/tests.py::TestSource -v

# Run specific test methods
python -m pytest tests/tests.py::TestMockedExecution::test_replication_run_success -v
```

### Using the Custom Test Runner

A custom test runner is provided that doesn't require pytest:

```bash
python run_tests.py
```

This runner tests the core functionality:
- Basic imports
- Mode enum
- Source class
- Target class
- ReplicationStream class
- Replication class
- JSON serialization

## Test Features

### Mocking
Most tests use mocking to avoid requiring the actual Go binary during testing. This allows:
- Fast test execution
- Testing without binary dependencies
- Controlled test scenarios
- Exception simulation

### Fixtures
- `cleanup_temp_files`: Automatically cleans up temporary files created during tests

### Environment Testing
Tests verify that environment variables are properly merged and passed to the underlying binary execution.

### JSON Serialization Testing
Tests ensure that Python objects can be properly serialized to JSON configurations that the Go binary can understand.

## Test Coverage

The tests cover:
- ✅ All major classes (Source, Target, Replication, Pipeline, Task)
- ✅ Enum handling (Mode)
- ✅ Options classes (SourceOptions, TargetOptions)
- ✅ Hook system integration
- ✅ JSON serialization/deserialization
- ✅ Command preparation
- ✅ Environment variable handling
- ✅ Error scenarios and edge cases
- ✅ Temp file management
- ✅ Stream management (enable/disable)
- ✅ Configuration validation

## Dependencies

- `pytest` (for running the full test suite)
- `pytest-mock` (for mocking functionality)
- Standard library modules: `os`, `json`, `tempfile`, `unittest.mock`

## Notes

- Tests are designed to work without requiring the actual Sling Go binary
- Integration tests that require the binary are automatically skipped if not available
- All tests use mocking for binary execution to ensure fast, reliable testing
- Temporary files are automatically cleaned up after tests complete 