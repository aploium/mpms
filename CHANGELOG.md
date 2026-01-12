# MPMS Changelog

## Version 2.5.4.0

### Fixes
- Fix error when `p.is_alive()` raises exception

## Version 2.5.0 (Unreleased)

### New Features
- **Iterator-based Result Collection**: Added `iter_results()` method as an alternative to the collector pattern
  - Provides a more Pythonic way to process task results
  - Supports timeout parameter for result retrieval
  - Cannot be used together with collector parameter
  - Must call `close()` before using `iter_results()`
  - Automatically handles Meta object creation and cleanup

### Improvements
- Result queue is now always created to support both collector and iter_results patterns
- Enhanced task tracking to support iter_results when collector is not specified
- Added comprehensive error handling for iter_results edge cases

### Examples
- Added `demo_iter_results.py` with multiple usage scenarios
- Added `test_iter_results.py` for testing the new functionality
- Added `example_iter_results_simple.py` as a simple demonstration

## Version 2.4.1
- Previous release (baseline for changes)

## Version 2.2.0
- Added lifecycle management features
  - Count-based lifecycle control
  - Time-based lifecycle control
  - Hard timeout limits for processes and tasks 
