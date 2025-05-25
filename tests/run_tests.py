#!/usr/bin/env python3
# coding=utf-8
"""Convenient test runner for MPMS tests"""

import sys
import os
import argparse
import subprocess

# Add parent directory to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def run_tests(args):
    """Run tests with specified options"""
    cmd = [sys.executable, '-m', 'pytest']
    
    # Add verbose flag if requested
    if args.verbose:
        cmd.append('-v')
    
    # Add coverage flag if requested
    if args.coverage:
        cmd.extend(['--cov=mpms', '--cov-report=term-missing'])
    
    # Add marker filter
    if args.markers:
        cmd.extend(['-m', args.markers])
    
    # Add specific test file
    if args.file:
        cmd.append(args.file)
    
    # Add any additional pytest arguments
    if args.pytest_args:
        cmd.extend(args.pytest_args)
    
    # Run the tests
    print(f"Running: {' '.join(cmd)}")
    return subprocess.call(cmd)


def main():
    parser = argparse.ArgumentParser(description='Run MPMS tests')
    
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Verbose output')
    
    parser.add_argument('-c', '--coverage', action='store_true',
                        help='Run with coverage report')
    
    parser.add_argument('-m', '--markers', type=str,
                        help='Run tests matching given mark expression (e.g., "not slow")')
    
    parser.add_argument('-f', '--file', type=str,
                        help='Run specific test file')
    
    parser.add_argument('pytest_args', nargs='*',
                        help='Additional arguments to pass to pytest')
    
    # Add common test scenarios
    parser.add_argument('--quick', action='store_true',
                        help='Run only quick tests (exclude slow/performance tests)')
    
    parser.add_argument('--basic', action='store_true',
                        help='Run only basic functionality tests')
    
    parser.add_argument('--lifecycle', action='store_true',
                        help='Run only lifecycle tests')
    
    parser.add_argument('--performance', action='store_true',
                        help='Run only performance tests')
    
    parser.add_argument('--all', action='store_true',
                        help='Run all tests including slow ones')
    
    args = parser.parse_args()
    
    # Handle common scenarios
    if args.quick:
        args.markers = 'not slow and not performance'
    elif args.basic:
        args.file = 'test_mpms_basic.py'
    elif args.lifecycle:
        args.file = 'test_mpms_lifecycle.py'
    elif args.performance:
        args.file = 'test_mpms_performance.py'
    elif args.all:
        args.markers = None  # Run everything
    
    # Change to tests directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    # Run tests
    sys.exit(run_tests(args))


if __name__ == '__main__':
    main() 