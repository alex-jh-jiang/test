# test

A simple Python calculator module with unit tests.

## Project Structure

- `calculator.py` - Main calculator module with basic arithmetic operations
- `test_calculator.py` - Unit tests for the calculator module

## Running Tests

Run the tests using Python's unittest framework:

```bash
python test_calculator.py
```

Or run with verbose output:

```bash
python test_calculator.py -v
```

## Functions

The calculator module provides the following functions:

- `add(a, b)` - Add two numbers
- `subtract(a, b)` - Subtract b from a
- `multiply(a, b)` - Multiply two numbers
- `divide(a, b)` - Divide a by b (raises ValueError if b is 0)