"""A simple calculator module for demonstration."""

from typing import Union


def add(a: Union[int, float], b: Union[int, float]) -> Union[int, float]:
    """Add two numbers and return the result."""
    return a + b


def subtract(a: Union[int, float], b: Union[int, float]) -> Union[int, float]:
    """Subtract b from a and return the result."""
    return a - b


def multiply(a: Union[int, float], b: Union[int, float]) -> Union[int, float]:
    """Multiply two numbers and return the result."""
    return a * b


def divide(a: Union[int, float], b: Union[int, float]) -> float:
    """Divide a by b and return the result.
    
    Args:
        a: The dividend
        b: The divisor
    
    Returns:
        The quotient of a divided by b
    
    Raises:
        ValueError: If b is zero
    """
    if b == 0:
        raise ValueError("Cannot divide by zero")
    return a / b
