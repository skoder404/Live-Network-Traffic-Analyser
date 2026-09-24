"""
streaming/analytics/filters.py — Stream filtering specification and expression builder.

Parses config-defined named filters, validates SQL expressions against the data contract schema,
and provides helper functions to produce filtered streaming views.
Reference: TECH_RULES §3.5, todo.md T4-006
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

from contracts.record_schema import to_spark_schema

logger = logging.getLogger("StreamFilters")

DEFAULT_FILTERS = [
    {"name": "tcp_only", "expr": "protocol = 'TCP'"},
    {"name": "udp_only", "expr": "protocol = 'UDP'"},
    {"name": "web_traffic", "expr": "dst_port IN (80, 443)"},
    {"name": "dns_traffic", "expr": "dst_port = 53"},
    {"name": "large_packets", "expr": "packet_length > 1000"},
]


@dataclass(frozen=True)
class FilterSpec:
    """Specification for a named streaming filter."""

    name: str
    expr: str

    def __post_init__(self) -> None:
        if not self.name or not isinstance(self.name, str):
            raise ValueError("FilterSpec.name must be a non-empty string")
        if not self.expr or not isinstance(self.expr, str):
            raise ValueError("FilterSpec.expr must be a non-empty string")


def parse_filters(cfg: dict[str, Any] | list[Any] | None = None) -> list[FilterSpec]:
    """
    Parses filter definitions from config dictionary or list of specs.

    Supports:
      - Raw SQL expression: `{"name": "tcp_only", "expr": "protocol = 'TCP'"}`
      - Port list: `{"name": "web_traffic", "dst_ports": [80, 443]}`
      - Protocol: `{"name": "is_tcp", "protocol": "TCP"}`
      - Packet size: `{"name": "size_gt_1000", "min_length": 1000}`
      - Source IP: `{"name": "gateway", "src_ip": "192.168.1.1"}`

    Falls back to `DEFAULT_FILTERS` if configuration contains no filters.
    """
    filter_list: list[Any] = []
    if isinstance(cfg, dict):
        # Could be full config dict (e.g. cfg["spark"]["filters"]) or just spark section
        if "spark" in cfg and isinstance(cfg["spark"], dict):
            filter_list = cfg["spark"].get("filters", [])
        else:
            filter_list = cfg.get("filters", [])
    elif isinstance(cfg, list):
        filter_list = cfg

    if not filter_list:
        filter_list = DEFAULT_FILTERS

    specs: list[FilterSpec] = []
    for item in filter_list:
        if isinstance(item, FilterSpec):
            specs.append(item)
        elif isinstance(item, dict):
            name = item.get("name")
            if not name:
                raise ValueError(f"Filter item missing required 'name' key: {item}")

            if "expr" in item:
                expr = item["expr"]
            elif "dst_ports" in item:
                ports = item["dst_ports"]
                if isinstance(ports, (list, tuple)):
                    ports_str = ", ".join(str(int(p)) for p in ports)
                    expr = f"dst_port IN ({ports_str})"
                else:
                    expr = f"dst_port = {int(ports)}"
            elif "protocol" in item:
                proto = str(item["protocol"]).strip().upper()
                expr = f"protocol = '{proto}'"
            elif "min_length" in item:
                expr = f"packet_length >= {int(item['min_length'])}"
            elif "src_ip" in item:
                expr = f"src_ip = '{item['src_ip']}'"
            else:
                raise ValueError(
                    f"Filter '{name}' missing valid filter criteria (expr/dst_ports/protocol/min_length/src_ip)"
                )

            specs.append(FilterSpec(name=str(name), expr=str(expr)))
        else:
            raise TypeError(f"Invalid filter specification type: {type(item)}")

    return specs


def validate_filters(filters: list[FilterSpec], spark: SparkSession | None = None) -> None:
    """
    Validates each filter expression at startup by compiling against the contract schema.
    Raises ValueError with descriptive message if an expression contains invalid syntax
    or refers to non-existent columns.
    """
    from pyspark.sql import functions as F

    if spark is None:
        from streaming.common.session import get_spark

        spark = get_spark("LNTA-FilterValidator")

    schema = to_spark_schema()
    empty_df = spark.createDataFrame([], schema=schema)

    for f in filters:
        try:
            # Test compiling and filtering against schema
            empty_df.filter(F.expr(f.expr)).count()
        except Exception as exc:
            logger.error("Failed to compile filter '%s' with expr '%s': %s", f.name, f.expr, exc)
            raise ValueError(
                f"Invalid filter expression for filter '{f.name}': \"{f.expr}\". Error: {exc}"
            ) from exc


def apply_filter(
    df: DataFrame,
    filter_target: FilterSpec | str,
    filters: list[FilterSpec] | None = None,
) -> DataFrame:
    """
    Applies the named filter specification to a streaming or static DataFrame.

    Args:
        df: Input packet DataFrame.
        filter_target: FilterSpec instance or string filter name.
        filters: Optional list of available FilterSpec instances for lookup.

    Returns:
        Filtered DataFrame.
    """
    from pyspark.sql import functions as F

    if isinstance(filter_target, FilterSpec):
        return df.filter(F.expr(filter_target.expr))

    if isinstance(filter_target, str):
        if filters:
            for f in filters:
                if f.name == filter_target:
                    return df.filter(F.expr(f.expr))

        # If not found in provided list, try checking DEFAULT_FILTERS
        default_specs = parse_filters(DEFAULT_FILTERS)
        for f in default_specs:
            if f.name == filter_target:
                return df.filter(F.expr(f.expr))

        raise KeyError(f"Filter '{filter_target}' not found in available filters")

    raise TypeError(f"Expected FilterSpec or str, got {type(filter_target)}")
