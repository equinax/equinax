"""Dynamic strategy loader for executing user-defined strategies."""

import ast
import sys
from typing import Dict, Any, Type, Optional
from io import StringIO
import backtrader as bt

# Restricted builtins for sandboxed execution
SAFE_BUILTINS = {
    '__build_class__': __build_class__,  # Required for class definitions
    '__name__': '__main__',
    'abs': abs,
    'all': all,
    'any': any,
    'bool': bool,
    'dict': dict,
    'enumerate': enumerate,
    'filter': filter,
    'float': float,
    'format': format,
    'frozenset': frozenset,
    'getattr': getattr,
    'hasattr': hasattr,
    'hash': hash,
    'int': int,
    'isinstance': isinstance,
    'issubclass': issubclass,
    'iter': iter,
    'len': len,
    'list': list,
    'map': map,
    'max': max,
    'min': min,
    'next': next,
    'object': object,  # Required for class inheritance
    'pow': pow,
    'print': print,
    'property': property,
    'range': range,
    'repr': repr,
    'reversed': reversed,
    'round': round,
    'set': set,
    'setattr': setattr,
    'slice': slice,
    'sorted': sorted,
    'staticmethod': staticmethod,
    'classmethod': classmethod,
    'str': str,
    'sum': sum,
    'super': super,  # Required for class inheritance
    'tuple': tuple,
    'type': type,
    'zip': zip,
    'True': True,
    'False': False,
    'None': None,
}

# Forbidden AST node types
FORBIDDEN_NODES = {
    ast.Import,
    ast.ImportFrom,
    ast.Global,
    ast.Nonlocal,
}

# Allowed module imports (whitelisted)
ALLOWED_IMPORTS = {
    'math',
    'statistics',
    'datetime',
    'collections',
    'itertools',
    'functools',
    'operator',
}


class StrategyValidationError(Exception):
    """Raised when strategy code validation fails."""
    pass


class StrategyLoader:
    """
    Loads and validates user-defined trading strategies.

    Security features:
    - AST-based code validation
    - Restricted builtins
    - Import whitelist
    """

    @staticmethod
    def validate_code(code: str) -> Dict[str, Any]:
        """
        Validate strategy code for security and correctness.

        Args:
            code: Python code string

        Returns:
            Dict with validation result and any warnings/errors

        Raises:
            StrategyValidationError: If code is invalid or dangerous
        """
        result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'has_strategy_class': False,
            'strategy_class_name': None,
        }

        # Parse AST
        try:
            tree = ast.parse(code)
        except SyntaxError as e:
            result['valid'] = False
            result['errors'].append(f"Syntax error: {e}")
            return result

        # Check for forbidden nodes
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name not in ALLOWED_IMPORTS:
                        result['errors'].append(
                            f"Import not allowed: {alias.name}. "
                            f"Allowed imports: {', '.join(ALLOWED_IMPORTS)}"
                        )
                        result['valid'] = False

            elif isinstance(node, ast.ImportFrom):
                if node.module not in ALLOWED_IMPORTS:
                    result['errors'].append(
                        f"Import from not allowed: {node.module}"
                    )
                    result['valid'] = False

            elif isinstance(node, (ast.Global, ast.Nonlocal)):
                result['warnings'].append(
                    "Global/nonlocal statements are discouraged"
                )

            # Check for dangerous attribute access
            elif isinstance(node, ast.Attribute):
                if node.attr.startswith('_'):
                    result['warnings'].append(
                        f"Access to private attribute '{node.attr}' is discouraged"
                    )

            # Check for exec/eval calls
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    if node.func.id in ('exec', 'eval', 'compile', '__import__'):
                        result['errors'].append(
                            f"Function '{node.func.id}' is not allowed"
                        )
                        result['valid'] = False

        # Check for bt.Strategy subclass
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                for base in node.bases:
                    # Check for bt.Strategy or Strategy base
                    if isinstance(base, ast.Attribute):
                        if base.attr == 'Strategy':
                            result['has_strategy_class'] = True
                            result['strategy_class_name'] = node.name
                    elif isinstance(base, ast.Name):
                        if base.id == 'Strategy':
                            result['has_strategy_class'] = True
                            result['strategy_class_name'] = node.name

        if not result['has_strategy_class']:
            result['errors'].append(
                "Code must define a class that inherits from bt.Strategy"
            )
            result['valid'] = False

        return result

    @staticmethod
    def load_strategy(
        code: str,
        strategy_name: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> Type[bt.Strategy]:
        """
        Load a strategy class from code string.

        Args:
            code: Python code string defining a bt.Strategy subclass
            strategy_name: Name for the strategy class
            parameters: Optional parameters to override defaults

        Returns:
            Strategy class (not instance)

        Raises:
            StrategyValidationError: If code validation fails
        """
        # Validate first
        validation = StrategyLoader.validate_code(code)
        if not validation['valid']:
            raise StrategyValidationError(
                f"Strategy validation failed: {'; '.join(validation['errors'])}"
            )

        # Create execution namespace with safe builtins and backtrader
        namespace = {
            '__builtins__': SAFE_BUILTINS,
            'bt': bt,
        }

        # Add allowed imports
        import math
        import statistics
        import datetime
        import collections
        import itertools
        import functools
        import operator

        namespace.update({
            'math': math,
            'statistics': statistics,
            'datetime': datetime,
            'collections': collections,
            'itertools': itertools,
            'functools': functools,
            'operator': operator,
        })

        # Execute code in sandboxed namespace
        try:
            exec(code, namespace)
        except Exception as e:
            raise StrategyValidationError(f"Failed to execute strategy code: {e}")

        # Find the strategy class
        strategy_class = None
        class_name = validation['strategy_class_name']

        if class_name and class_name in namespace:
            strategy_class = namespace[class_name]
        else:
            # Find any bt.Strategy subclass
            for name, obj in namespace.items():
                if (isinstance(obj, type) and
                    issubclass(obj, bt.Strategy) and
                    obj is not bt.Strategy):
                    strategy_class = obj
                    break

        if strategy_class is None:
            raise StrategyValidationError("No valid strategy class found in code")

        # Apply parameter overrides if provided
        if parameters:
            strategy_class = StrategyLoader._create_parameterized_strategy(
                strategy_class, parameters
            )

        return strategy_class

    @staticmethod
    def _create_parameterized_strategy(
        base_class: Type[bt.Strategy],
        parameters: Dict[str, Any],
    ) -> Type[bt.Strategy]:
        """
        Create a new strategy class with overridden parameters.

        Args:
            base_class: Original strategy class
            parameters: Parameters to override

        Returns:
            New strategy class with updated params
        """
        # Get existing params
        existing_params = dict(base_class.params._getitems()) if hasattr(base_class, 'params') else {}

        # Merge with overrides
        merged_params = {**existing_params, **parameters}

        # Create new params tuple
        params_tuple = tuple(merged_params.items())

        # Create new class with updated params
        new_class = type(
            base_class.__name__,
            (base_class,),
            {'params': params_tuple}
        )

        return new_class


# Example strategy templates
STRATEGY_TEMPLATES = {
    'sma_crossover': '''
class SMACrossover(bt.Strategy):
    """Simple Moving Average Crossover Strategy"""

    params = (
        ('fast_period', 10),
        ('slow_period', 30),
    )

    def __init__(self):
        self.fast_ma = bt.indicators.SMA(self.data.close, period=self.p.fast_period)
        self.slow_ma = bt.indicators.SMA(self.data.close, period=self.p.slow_period)
        self.crossover = bt.indicators.CrossOver(self.fast_ma, self.slow_ma)

    def next(self):
        if not self.position:
            if self.crossover > 0:
                self.buy()
        elif self.crossover < 0:
            self.close()
''',

    'rsi_strategy': '''
class RSIStrategy(bt.Strategy):
    """RSI Overbought/Oversold Strategy"""

    params = (
        ('rsi_period', 14),
        ('overbought', 70),
        ('oversold', 30),
    )

    def __init__(self):
        self.rsi = bt.indicators.RSI(self.data.close, period=self.p.rsi_period)

    def next(self):
        if not self.position:
            if self.rsi < self.p.oversold:
                self.buy()
        else:
            if self.rsi > self.p.overbought:
                self.close()
''',

    'macd_strategy': '''
class MACDStrategy(bt.Strategy):
    """MACD Crossover Strategy"""

    params = (
        ('fast_period', 12),
        ('slow_period', 26),
        ('signal_period', 9),
    )

    def __init__(self):
        self.macd = bt.indicators.MACD(
            self.data.close,
            period_me1=self.p.fast_period,
            period_me2=self.p.slow_period,
            period_signal=self.p.signal_period,
        )

    def next(self):
        if not self.position:
            if self.macd.macd > self.macd.signal:
                self.buy()
        elif self.macd.macd < self.macd.signal:
            self.close()
''',

    'bollinger_bands': '''
class BollingerBandsStrategy(bt.Strategy):
    """Bollinger Bands Mean Reversion Strategy"""

    params = (
        ('period', 20),
        ('devfactor', 2.0),
    )

    def __init__(self):
        self.boll = bt.indicators.BollingerBands(
            self.data.close,
            period=self.p.period,
            devfactor=self.p.devfactor,
        )

    def next(self):
        if not self.position:
            if self.data.close < self.boll.lines.bot:
                self.buy()
        else:
            if self.data.close > self.boll.lines.mid:
                self.close()
''',
}
