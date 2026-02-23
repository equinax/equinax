from __future__ import annotations

import ast
import functools
import operator
import re
from pathlib import Path
from typing import Any, Literal

import polars as pl
import yaml

CONFIGS_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent / "alpha_lab" / "configs"

ScreenerTabKey = Literal["weekly", "rally", "dragon", "overnight"]
VALID_TABS: frozenset[str] = frozenset(("weekly", "rally", "dragon", "overnight"))

_SAFE_OPERATORS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.USub: operator.neg,
}


class ConfigLoadError(Exception):
    pass


class StrategyConfigYAML:
    __slots__ = (
        "strategy",
        "version",
        "label_cn",
        "description",
        "score_column",
        "eval_period",
        "requires_moneyflow",
        "backtest_top_n",
        "positive_factors",
        "penalties",
        "pre_filters",
        "regime_discount_enabled",
        "parameter_bounds",
        "market_gate",
    )

    def __init__(self, data: dict[str, Any]):
        self.strategy: str = data["strategy"]
        self.version: str = str(data["version"])
        self.label_cn: str = data.get("label_cn", "")
        self.description: str = data.get("description", "")
        self.score_column: str = data["score_column"]
        self.eval_period: int = data.get("eval_period", 20)
        self.requires_moneyflow: bool = data.get("requires_moneyflow", False)
        self.backtest_top_n: int = data.get("backtest_top_n", 5)

        scoring = data.get("scoring", {})
        self.positive_factors: list[dict] = scoring.get("positive_factors", [])
        self.penalties: list[dict] = scoring.get("penalties", [])
        self.pre_filters: list[dict] = scoring.get("pre_filters", [])
        self.regime_discount_enabled: bool = scoring.get("regime_discount", {}).get("enabled", True)
        self.parameter_bounds: dict = data.get("parameter_bounds", {})
        self.market_gate: dict = data.get("market_gate", {})


def _version_sort_key(filename: str) -> tuple[int, ...]:
    """Extract numeric parts from version filename for sorting.

    'v8.0.yaml' -> (8, 0), 'v22.yaml' -> (22,), 'v3b.yaml' -> (3,)
    """
    stem = Path(filename).stem
    nums = re.findall(r"\d+", stem)
    return tuple(int(n) for n in nums) if nums else (0,)


def _resolve_head_version(strategy_dir: Path) -> Path:
    yamls = sorted(strategy_dir.glob("v*.yaml"), key=lambda p: _version_sort_key(p.name))
    if not yamls:
        raise ConfigLoadError(f"No version files found in {strategy_dir}")
    for yf in yamls:
        with open(yf) as f:
            data = yaml.safe_load(f)
        if data.get("head") is True:
            return yf
    return yamls[-1]


@functools.lru_cache(maxsize=32)
def load_strategy_config(
    strategy: str,
    version: str | None = None,
    config_dir: Path | None = None,
) -> StrategyConfigYAML:
    config_dir = config_dir or CONFIGS_DIR
    strategy_dir = config_dir / strategy

    if strategy_dir.is_dir():
        if version is None:
            path = _resolve_head_version(strategy_dir)
        else:
            path = strategy_dir / f"v{version}.yaml"
            if not path.exists():
                raise ConfigLoadError(f"Version file not found: {path}")
    else:
        path = config_dir / f"{strategy}.yaml"
        if not path.exists():
            raise ConfigLoadError(f"Config not found for strategy '{strategy}' in {config_dir}")

    with open(path) as f:
        data = yaml.safe_load(f)
    if data.get("strategy") != strategy:
        raise ConfigLoadError(
            f"Strategy mismatch: file says '{data.get('strategy')}', expected '{strategy}'"
        )
    return StrategyConfigYAML(data)


def list_strategy_versions(
    strategy: str,
    config_dir: Path | None = None,
) -> list[dict[str, Any]]:
    config_dir = config_dir or CONFIGS_DIR
    strategy_dir = config_dir / strategy

    if not strategy_dir.is_dir():
        cfg = load_strategy_config(strategy, config_dir=config_dir)
        return [
            {
                "version": cfg.version,
                "label_cn": cfg.label_cn,
                "description": cfg.description,
                "is_head": True,
            }
        ]

    yamls = sorted(strategy_dir.glob("v*.yaml"), key=lambda p: _version_sort_key(p.name))
    if not yamls:
        raise ConfigLoadError(f"No version files found in {strategy_dir}")

    head_path = _resolve_head_version(strategy_dir)
    versions: list[dict[str, Any]] = []
    for yf in yamls:
        with open(yf) as f:
            data = yaml.safe_load(f)
        versions.append(
            {
                "version": str(data.get("version", yf.stem)),
                "label_cn": data.get("label_cn", ""),
                "description": data.get("description", ""),
                "is_head": yf == head_path,
            }
        )
    return versions


def compile_transform(transform_str: str, source_col: str, fill_null: float) -> pl.Expr:
    """Compile a restricted transform DSL string into a Polars expression.

    The DSL uses ``col`` as placeholder for ``pl.col(source_col).fill_null(fill_null)``.
    Supported operations:
      - Arithmetic: ``+  -  *  /``
      - Methods on ``col``: ``.clip(lo, hi)``, ``.abs()``
      - Parentheses for grouping
      - Numeric literals (int / float)
    """
    base_expr = pl.col(source_col).fill_null(fill_null)
    tree = ast.parse(transform_str, mode="eval")
    return _eval_node(tree.body, base_expr)


def _eval_node(node: ast.AST, col_expr: pl.Expr) -> pl.Expr:
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return pl.lit(node.value)

    if isinstance(node, ast.Name) and node.id == "col":
        return col_expr

    if isinstance(node, ast.UnaryOp) and type(node.op) in _SAFE_OPERATORS:
        operand = _eval_node(node.operand, col_expr)
        return _SAFE_OPERATORS[type(node.op)](operand)

    if isinstance(node, ast.BinOp) and type(node.op) in _SAFE_OPERATORS:
        left = _eval_node(node.left, col_expr)
        right = _eval_node(node.right, col_expr)
        return _SAFE_OPERATORS[type(node.op)](left, right)

    if isinstance(node, ast.Call):
        return _eval_call(node, col_expr)

    raise ConfigLoadError(f"Unsupported AST node in transform: {ast.dump(node)}")


def _eval_call(node: ast.Call, col_expr: pl.Expr) -> pl.Expr:
    if not isinstance(node.func, ast.Attribute):
        raise ConfigLoadError(f"Only method calls are allowed, got: {ast.dump(node.func)}")

    method_name = node.func.attr
    receiver = _eval_node(node.func.value, col_expr)

    if method_name == "clip":
        if len(node.args) != 2:
            raise ConfigLoadError("clip() requires exactly 2 arguments")
        lo_val = _require_numeric_literal(node.args[0])
        hi_val = _require_numeric_literal(node.args[1])
        return receiver.clip(lo_val, hi_val)

    if method_name == "abs":
        if node.args:
            raise ConfigLoadError("abs() takes no arguments")
        return receiver.abs()

    raise ConfigLoadError(f"Unsupported method: {method_name}")


def _require_numeric_literal(node: ast.AST) -> float:
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return float(node.value)
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        inner = _require_numeric_literal(node.operand)
        return -inner
    raise ConfigLoadError(f"clip() arguments must be numeric literals, got: {ast.dump(node)}")


def _build_component_expr(factor: dict) -> pl.Expr:
    source = factor["source"]
    fill_null = factor.get("fill_null", 0.0)
    signal = factor["signal"]
    transform = factor.get("transform")

    if transform:
        return compile_transform(transform, source, fill_null).alias(signal)
    return pl.col(source).fill_null(fill_null).alias(signal)


def build_scoring_expressions(
    config: StrategyConfigYAML,
) -> tuple[list[pl.Expr], pl.Expr]:
    """Build Polars component column expressions and the weighted sum expression.

    Returns:
        (component_exprs, raw_score_expr)
        - component_exprs: list of aliased expressions for with_columns()
        - raw_score_expr: the final weighted-sum expression (not yet aliased)
    """
    component_exprs: list[pl.Expr] = []
    score_terms: list[pl.Expr] = []

    for factor in config.positive_factors:
        expr = _build_component_expr(factor)
        component_exprs.append(expr)
        score_terms.append(pl.col(factor["signal"]) * factor["weight"])

    for penalty in config.penalties:
        expr = _build_component_expr(penalty)
        component_exprs.append(expr)
        score_terms.append(-(pl.col(penalty["signal"]) * penalty["weight"]))

    if not score_terms:
        raise ConfigLoadError(f"Strategy '{config.strategy}' has no scoring terms")

    raw_score = score_terms[0]
    for term in score_terms[1:]:
        raw_score = raw_score + term

    return component_exprs, raw_score


def build_pre_filter_mask(config: StrategyConfigYAML) -> pl.Expr | None:
    """Build a boolean mask from pre_filters. Returns None if no filters."""
    if not config.pre_filters:
        return None

    ops = {
        "gte": operator.ge,
        "lte": operator.le,
        "gt": operator.gt,
        "lt": operator.lt,
        "eq": operator.eq,
        "neq": operator.ne,
    }

    masks: list[pl.Expr] = []
    for f in config.pre_filters:
        col = f["column"]
        op_name = f["op"]
        value = f["value"]
        fill = f.get("fill_null", 0.0)

        if op_name not in ops:
            raise ConfigLoadError(f"Unknown filter op: {op_name}")

        masks.append(ops[op_name](pl.col(col).fill_null(fill), value))

    result = masks[0]
    for m in masks[1:]:
        result = result & m
    return result


def score_from_config(
    config: StrategyConfigYAML,
    df: pl.DataFrame,
    regime_discount_fn,
) -> pl.DataFrame:
    """Full scoring pipeline driven by YAML config.

    Args:
        config: parsed strategy config
        df: DataFrame with _ensure_columns already applied
        regime_discount_fn: bound method ScoringEngine._apply_regime_discount
    """
    if df.is_empty():
        return df

    component_exprs, raw_score = build_scoring_expressions(config)
    df = df.with_columns(component_exprs)

    clipped = raw_score.clip(0.0, 100.0)

    pre_filter = build_pre_filter_mask(config)
    if pre_filter is not None:
        clipped = pl.when(pre_filter).then(clipped).otherwise(pl.lit(0.0))

    if config.regime_discount_enabled:
        final = regime_discount_fn(clipped)
    else:
        final = clipped

    df = df.with_columns([final.alias(config.score_column)])
    return df
