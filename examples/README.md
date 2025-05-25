# MPMS Examples

This directory contains example scripts demonstrating various features of MPMS.

## Files

### demo.py
The original comprehensive demo showing basic MPMS usage with multiple workers and collectors.

### demo_lifecycle.py
Demonstrates the new lifecycle features:
- **Count-based lifecycle**: Worker threads exit after processing a specific number of tasks
- **Time-based lifecycle**: Worker threads exit after running for a specific duration
- **Combined lifecycle**: Using both count and time limits together

## Running the Examples

```bash
# Run the basic demo
python examples/demo.py

# Run the lifecycle demo
python examples/demo_lifecycle.py
``` 