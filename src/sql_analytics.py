"""
Advanced SQL Analytics Toolkit

Window functions, CTEs, recursive queries, pivot tables, running totals,
moving averages, rank functions, and a fluent query builder -- all backed
by Python's built-in sqlite3 module.

Author: Gabriel Demetrios Lafis
"""

import sqlite3
import math
from typing import Any, Callable, Dict, List, Optional, Tuple, Union


# ── SQLite Analytics Database ────────────────────────────────────────

class AnalyticsDB:
    """Lightweight analytics database backed by SQLite."""

    def __init__(self, db_path: str = ":memory:"):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

    def execute(self, sql: str, params: tuple = ()) -> List[Dict]:
        cur = self.conn.execute(sql, params)
        if cur.description is None:
            self.conn.commit()
            return []
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def execute_script(self, sql: str) -> None:
        self.conn.executescript(sql)

    def load_rows(self, table: str, rows: List[Dict], if_exists: str = "replace") -> int:
        if not rows:
            return 0
        cols = list(rows[0].keys())
        col_defs = ", ".join(f'"{c}" TEXT' for c in cols)
        if if_exists == "replace":
            self.conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        self.conn.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({col_defs})')
        placeholders = ", ".join("?" for _ in cols)
        for row in rows:
            vals = [row.get(c) for c in cols]
            self.conn.execute(
                f'INSERT INTO "{table}" ({", ".join(f"{c}" for c in cols)}) VALUES ({placeholders})',
                vals,
            )
        self.conn.commit()
        return len(rows)

    def tables(self) -> List[str]:
        rows = self.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        return [r["name"] for r in rows]

    def columns(self, table: str) -> List[str]:
        rows = self.execute(f'PRAGMA table_info("{table}")')
        return [r["name"] for r in rows]

    def close(self):
        self.conn.close()


# ── Window Functions (pure-Python emulation) ─────────────────────────

class WindowFunctions:
    """Pure-Python window functions over list-of-dict datasets."""

    @staticmethod
    def row_number(rows: List[Dict], order_by: str, partition_by: Optional[str] = None) -> List[Dict]:
        groups = _partition(rows, partition_by)
        out = []
        for group in groups.values():
            sorted_g = sorted(group, key=lambda r: r.get(order_by, ""))
            for i, r in enumerate(sorted_g, 1):
                nr = dict(r)
                nr["row_number"] = i
                out.append(nr)
        return out

    @staticmethod
    def rank(rows: List[Dict], order_by: str, partition_by: Optional[str] = None) -> List[Dict]:
        groups = _partition(rows, partition_by)
        out = []
        for group in groups.values():
            sorted_g = sorted(group, key=lambda r: r.get(order_by, ""))
            rank = 1
            for i, r in enumerate(sorted_g):
                if i > 0 and sorted_g[i].get(order_by) != sorted_g[i - 1].get(order_by):
                    rank = i + 1
                nr = dict(r)
                nr["rank"] = rank
                out.append(nr)
        return out

    @staticmethod
    def dense_rank(rows: List[Dict], order_by: str, partition_by: Optional[str] = None) -> List[Dict]:
        groups = _partition(rows, partition_by)
        out = []
        for group in groups.values():
            sorted_g = sorted(group, key=lambda r: r.get(order_by, ""))
            rank = 1
            for i, r in enumerate(sorted_g):
                if i > 0 and sorted_g[i].get(order_by) != sorted_g[i - 1].get(order_by):
                    rank += 1
                nr = dict(r)
                nr["dense_rank"] = rank
                out.append(nr)
        return out

    @staticmethod
    def ntile(rows: List[Dict], n: int, order_by: str, partition_by: Optional[str] = None) -> List[Dict]:
        groups = _partition(rows, partition_by)
        out = []
        for group in groups.values():
            sorted_g = sorted(group, key=lambda r: r.get(order_by, ""))
            size = len(sorted_g)
            for i, r in enumerate(sorted_g):
                nr = dict(r)
                nr["ntile"] = (i * n) // size + 1
                out.append(nr)
        return out

    @staticmethod
    def lag(rows: List[Dict], column: str, offset: int = 1,
            order_by: str = "", partition_by: Optional[str] = None,
            default: Any = None) -> List[Dict]:
        groups = _partition(rows, partition_by)
        out = []
        for group in groups.values():
            sorted_g = sorted(group, key=lambda r: r.get(order_by, ""))
            for i, r in enumerate(sorted_g):
                nr = dict(r)
                nr[f"lag_{column}"] = sorted_g[i - offset].get(column) if i >= offset else default
                out.append(nr)
        return out

    @staticmethod
    def lead(rows: List[Dict], column: str, offset: int = 1,
             order_by: str = "", partition_by: Optional[str] = None,
             default: Any = None) -> List[Dict]:
        groups = _partition(rows, partition_by)
        out = []
        for group in groups.values():
            sorted_g = sorted(group, key=lambda r: r.get(order_by, ""))
            for i, r in enumerate(sorted_g):
                idx = i + offset
                nr = dict(r)
                nr[f"lead_{column}"] = sorted_g[idx].get(column) if idx < len(sorted_g) else default
                out.append(nr)
        return out


def _partition(rows: List[Dict], partition_by: Optional[str]) -> Dict[Any, List[Dict]]:
    if partition_by is None:
        return {"__all__": list(rows)}
    groups: Dict[Any, List[Dict]] = {}
    for r in rows:
        key = r.get(partition_by, "__null__")
        groups.setdefault(key, []).append(r)
    return groups


# ── Aggregation Helpers ──────────────────────────────────────────────

class AggFunctions:
    """Common aggregate helpers operating on list-of-dict."""

    @staticmethod
    def running_total(rows: List[Dict], value_col: str, order_by: str,
                      partition_by: Optional[str] = None) -> List[Dict]:
        groups = _partition(rows, partition_by)
        out = []
        for group in groups.values():
            sorted_g = sorted(group, key=lambda r: r.get(order_by, ""))
            total = 0.0
            for r in sorted_g:
                total += float(r.get(value_col, 0))
                nr = dict(r)
                nr["running_total"] = round(total, 4)
                out.append(nr)
        return out

    @staticmethod
    def moving_average(rows: List[Dict], value_col: str, window: int,
                       order_by: str, partition_by: Optional[str] = None) -> List[Dict]:
        groups = _partition(rows, partition_by)
        out = []
        for group in groups.values():
            sorted_g = sorted(group, key=lambda r: r.get(order_by, ""))
            values = [float(r.get(value_col, 0)) for r in sorted_g]
            for i, r in enumerate(sorted_g):
                start = max(0, i - window + 1)
                subset = values[start:i + 1]
                nr = dict(r)
                nr["moving_avg"] = round(sum(subset) / len(subset), 4) if subset else None
                out.append(nr)
        return out

    @staticmethod
    def percent_of_total(rows: List[Dict], value_col: str,
                         partition_by: Optional[str] = None) -> List[Dict]:
        groups = _partition(rows, partition_by)
        out = []
        for group in groups.values():
            total = sum(float(r.get(value_col, 0)) for r in group)
            for r in group:
                nr = dict(r)
                nr["pct_of_total"] = round(float(r.get(value_col, 0)) / total * 100, 2) if total else 0
                out.append(nr)
        return out

    @staticmethod
    def group_by_agg(rows: List[Dict], group_cols: List[str],
                     agg_specs: Dict[str, str]) -> List[Dict]:
        """
        Group rows and aggregate.
        agg_specs: {column: "sum"|"avg"|"count"|"min"|"max"}
        """
        buckets: Dict[tuple, List[Dict]] = {}
        for r in rows:
            key = tuple(r.get(c) for c in group_cols)
            buckets.setdefault(key, []).append(r)

        out = []
        for key, group in buckets.items():
            row = {c: k for c, k in zip(group_cols, key)}
            for col, fn in agg_specs.items():
                vals = [float(r.get(col, 0)) for r in group if r.get(col) is not None]
                if fn == "sum":
                    row[f"{col}_sum"] = round(sum(vals), 4)
                elif fn == "avg":
                    row[f"{col}_avg"] = round(sum(vals) / len(vals), 4) if vals else 0
                elif fn == "count":
                    row[f"{col}_count"] = len(vals)
                elif fn == "min":
                    row[f"{col}_min"] = min(vals) if vals else None
                elif fn == "max":
                    row[f"{col}_max"] = max(vals) if vals else None
            out.append(row)
        return out


# ── Pivot / Unpivot ──────────────────────────────────────────────────

class PivotTable:
    """Pivot and unpivot operations."""

    @staticmethod
    def pivot(rows: List[Dict], index_col: str, pivot_col: str,
              value_col: str, agg: str = "sum") -> List[Dict]:
        pivot_values = sorted({str(r.get(pivot_col, "")) for r in rows})
        buckets: Dict[Any, Dict[str, List[float]]] = {}
        for r in rows:
            idx = r.get(index_col)
            pv = str(r.get(pivot_col, ""))
            val = float(r.get(value_col, 0))
            if idx not in buckets:
                buckets[idx] = {v: [] for v in pivot_values}
            buckets[idx][pv].append(val)

        out = []
        for idx, cols in buckets.items():
            row = {index_col: idx}
            for pv, vals in cols.items():
                if agg == "sum":
                    row[pv] = round(sum(vals), 4) if vals else 0
                elif agg == "avg":
                    row[pv] = round(sum(vals) / len(vals), 4) if vals else 0
                elif agg == "count":
                    row[pv] = len(vals)
            out.append(row)
        return out

    @staticmethod
    def unpivot(rows: List[Dict], id_cols: List[str],
                value_cols: List[str], var_name: str = "variable",
                value_name: str = "value") -> List[Dict]:
        out = []
        for r in rows:
            base = {c: r.get(c) for c in id_cols}
            for vc in value_cols:
                nr = dict(base)
                nr[var_name] = vc
                nr[value_name] = r.get(vc)
                out.append(nr)
        return out


# ── CTE / Recursive Query Emulation ─────────────────────────────────

class CTERunner:
    """Emulate Common Table Expressions and recursive queries in Python."""

    @staticmethod
    def with_cte(base_query_fn: Callable[[], List[Dict]],
                 transform_fn: Callable[[List[Dict]], List[Dict]]) -> List[Dict]:
        """Non-recursive CTE: run base, then transform."""
        base = base_query_fn()
        return transform_fn(base)

    @staticmethod
    def recursive(seed_fn: Callable[[], List[Dict]],
                  step_fn: Callable[[List[Dict]], List[Dict]],
                  max_depth: int = 100) -> List[Dict]:
        """
        Recursive CTE emulation.
        seed_fn: produces initial rows.
        step_fn: given current frontier, produce next rows (empty = stop).
        """
        all_rows = seed_fn()
        frontier = list(all_rows)
        for _ in range(max_depth):
            new_rows = step_fn(frontier)
            if not new_rows:
                break
            all_rows.extend(new_rows)
            frontier = new_rows
        return all_rows


# ── Fluent Query Builder ─────────────────────────────────────────────

class QueryBuilder:
    """Fluent SQL query builder for SQLite."""

    def __init__(self, table: str):
        self._table = table
        self._select: List[str] = []
        self._where: List[str] = []
        self._group_by: List[str] = []
        self._having: List[str] = []
        self._order_by: List[str] = []
        self._limit: Optional[int] = None
        self._offset: Optional[int] = None
        self._joins: List[str] = []
        self._window: List[str] = []
        self._with_cte: List[Tuple[str, str]] = []
        self._params: List[Any] = []

    def select(self, *columns: str) -> "QueryBuilder":
        self._select.extend(columns)
        return self

    def where(self, condition: str, *params: Any) -> "QueryBuilder":
        self._where.append(condition)
        self._params.extend(params)
        return self

    def group_by(self, *columns: str) -> "QueryBuilder":
        self._group_by.extend(columns)
        return self

    def having(self, condition: str) -> "QueryBuilder":
        self._having.append(condition)
        return self

    def order_by(self, column: str, direction: str = "ASC") -> "QueryBuilder":
        self._order_by.append(f"{column} {direction}")
        return self

    def limit(self, n: int) -> "QueryBuilder":
        self._limit = n
        return self

    def offset(self, n: int) -> "QueryBuilder":
        self._offset = n
        return self

    def join(self, table: str, on: str, join_type: str = "INNER") -> "QueryBuilder":
        self._joins.append(f"{join_type} JOIN {table} ON {on}")
        return self

    def window(self, expr: str) -> "QueryBuilder":
        self._window.append(expr)
        return self

    def with_cte(self, name: str, query: str) -> "QueryBuilder":
        self._with_cte.append((name, query))
        return self

    def build(self) -> Tuple[str, List[Any]]:
        parts = []

        if self._with_cte:
            cte_parts = [f"{name} AS ({query})" for name, query in self._with_cte]
            parts.append("WITH " + ", ".join(cte_parts))

        select_clause = ", ".join(self._select) if self._select else "*"
        parts.append(f"SELECT {select_clause}")
        parts.append(f"FROM {self._table}")

        for j in self._joins:
            parts.append(j)

        if self._where:
            parts.append("WHERE " + " AND ".join(self._where))

        if self._group_by:
            parts.append("GROUP BY " + ", ".join(self._group_by))

        if self._having:
            parts.append("HAVING " + " AND ".join(self._having))

        if self._window:
            parts.append("WINDOW " + ", ".join(self._window))

        if self._order_by:
            parts.append("ORDER BY " + ", ".join(self._order_by))

        if self._limit is not None:
            parts.append(f"LIMIT {self._limit}")

        if self._offset is not None:
            parts.append(f"OFFSET {self._offset}")

        return " ".join(parts), self._params

    def run(self, db: AnalyticsDB) -> List[Dict]:
        sql, params = self.build()
        return db.execute(sql, tuple(params))


# ── Sample Data ──────────────────────────────────────────────────────

def generate_sales_data(n: int = 200, seed: int = 42) -> List[Dict]:
    """Deterministic sample sales data."""
    import random as _rnd
    _rnd.seed(seed)
    regions = ["North", "South", "East", "West"]
    products = ["Widget", "Gadget", "Doohickey", "Thingamajig"]
    rows = []
    for i in range(n):
        month = (i % 12) + 1
        rows.append({
            "id": i + 1,
            "date": f"2024-{month:02d}-{_rnd.randint(1,28):02d}",
            "region": _rnd.choice(regions),
            "product": _rnd.choice(products),
            "quantity": _rnd.randint(1, 50),
            "revenue": round(_rnd.uniform(10, 5000), 2),
            "cost": round(_rnd.uniform(5, 3000), 2),
        })
    return rows


def generate_org_hierarchy(depth: int = 4, branch: int = 2) -> List[Dict]:
    """Generate hierarchical org data for recursive CTE testing."""
    rows = [{"id": 1, "name": "CEO", "manager_id": None, "level": 0}]
    counter = 2
    parents = [1]
    for lv in range(1, depth):
        new_parents = []
        for pid in parents:
            for b in range(branch):
                rows.append({
                    "id": counter,
                    "name": f"Emp_{counter}",
                    "manager_id": pid,
                    "level": lv,
                })
                new_parents.append(counter)
                counter += 1
        parents = new_parents
    return rows
