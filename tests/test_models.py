"""
Tests for Advanced SQL Analytics Toolkit.

Author: Gabriel Demetrios Lafis
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from sql_analytics import (
    AnalyticsDB,
    WindowFunctions,
    AggFunctions,
    PivotTable,
    CTERunner,
    QueryBuilder,
    generate_sales_data,
    generate_org_hierarchy,
)


# ── Fixtures ─────────────────────────────────────────────────────────

@pytest.fixture
def sales_data():
    return generate_sales_data(100, seed=42)


@pytest.fixture
def db(sales_data):
    database = AnalyticsDB(":memory:")
    database.load_rows("sales", sales_data)
    yield database
    database.close()


@pytest.fixture
def org_data():
    return generate_org_hierarchy(depth=3, branch=2)


# ── AnalyticsDB Tests ────────────────────────────────────────────────

class TestAnalyticsDB:
    def test_load_and_query(self, db):
        rows = db.execute("SELECT COUNT(*) as cnt FROM sales")
        assert rows[0]["cnt"] == "100"

    def test_tables(self, db):
        assert "sales" in db.tables()

    def test_columns(self, db):
        cols = db.columns("sales")
        assert "id" in cols
        assert "revenue" in cols
        assert "region" in cols

    def test_execute_script(self):
        database = AnalyticsDB(":memory:")
        database.execute_script("""
            CREATE TABLE t (x TEXT);
            INSERT INTO t VALUES ('a');
            INSERT INTO t VALUES ('b');
        """)
        rows = database.execute("SELECT * FROM t")
        assert len(rows) == 2
        database.close()

    def test_load_empty(self):
        database = AnalyticsDB(":memory:")
        count = database.load_rows("empty", [])
        assert count == 0
        database.close()


# ── Window Functions Tests ───────────────────────────────────────────

class TestWindowFunctions:
    def test_row_number(self, sales_data):
        result = WindowFunctions.row_number(sales_data, "revenue")
        assert all("row_number" in r for r in result)
        nums = [r["row_number"] for r in result]
        assert sorted(nums) == list(range(1, len(sales_data) + 1))

    def test_row_number_partitioned(self, sales_data):
        result = WindowFunctions.row_number(sales_data, "revenue", partition_by="region")
        assert all("row_number" in r for r in result)

    def test_rank(self):
        rows = [{"v": 10}, {"v": 20}, {"v": 20}, {"v": 30}]
        result = WindowFunctions.rank(rows, "v")
        ranks = [r["rank"] for r in result]
        assert ranks == [1, 2, 2, 4]

    def test_dense_rank(self):
        rows = [{"v": 10}, {"v": 20}, {"v": 20}, {"v": 30}]
        result = WindowFunctions.dense_rank(rows, "v")
        dranks = [r["dense_rank"] for r in result]
        assert dranks == [1, 2, 2, 3]

    def test_ntile(self):
        rows = [{"v": i} for i in range(8)]
        result = WindowFunctions.ntile(rows, 4, "v")
        tiles = [r["ntile"] for r in result]
        assert min(tiles) == 1
        assert max(tiles) == 4

    def test_lag(self):
        rows = [{"id": i, "v": i * 10} for i in range(5)]
        result = WindowFunctions.lag(rows, "v", offset=1, order_by="id")
        assert result[0]["lag_v"] is None
        assert result[1]["lag_v"] == 0
        assert result[2]["lag_v"] == 10

    def test_lead(self):
        rows = [{"id": i, "v": i * 10} for i in range(5)]
        result = WindowFunctions.lead(rows, "v", offset=1, order_by="id")
        assert result[0]["lead_v"] == 10
        assert result[-1]["lead_v"] is None


# ── Aggregation Functions Tests ──────────────────────────────────────

class TestAggFunctions:
    def test_running_total(self):
        rows = [{"d": i, "v": 10} for i in range(5)]
        result = AggFunctions.running_total(rows, "v", "d")
        totals = [r["running_total"] for r in result]
        assert totals == [10, 20, 30, 40, 50]

    def test_running_total_partitioned(self, sales_data):
        result = AggFunctions.running_total(sales_data, "revenue", "date", partition_by="region")
        assert all("running_total" in r for r in result)

    def test_moving_average(self):
        rows = [{"d": i, "v": 10} for i in range(5)]
        result = AggFunctions.moving_average(rows, "v", 3, "d")
        assert result[0]["moving_avg"] == 10.0
        assert result[2]["moving_avg"] == 10.0

    def test_moving_average_window_size(self):
        rows = [{"d": i, "v": i * 10} for i in range(5)]
        result = AggFunctions.moving_average(rows, "v", 2, "d")
        assert result[0]["moving_avg"] == 0.0  # only one value
        assert result[1]["moving_avg"] == 5.0  # (0+10)/2

    def test_percent_of_total(self):
        rows = [{"v": 25}, {"v": 75}]
        result = AggFunctions.percent_of_total(rows, "v")
        pcts = sorted([r["pct_of_total"] for r in result])
        assert pcts == [25.0, 75.0]

    def test_group_by_agg(self, sales_data):
        result = AggFunctions.group_by_agg(
            sales_data, ["region"], {"revenue": "sum", "quantity": "avg"}
        )
        assert len(result) > 0
        assert "revenue_sum" in result[0]
        assert "quantity_avg" in result[0]

    def test_group_by_count(self):
        rows = [{"g": "a", "v": 1}, {"g": "a", "v": 2}, {"g": "b", "v": 3}]
        result = AggFunctions.group_by_agg(rows, ["g"], {"v": "count"})
        a_row = [r for r in result if r["g"] == "a"][0]
        assert a_row["v_count"] == 2

    def test_group_by_min_max(self):
        rows = [{"g": "x", "v": 5}, {"g": "x", "v": 15}, {"g": "x", "v": 10}]
        result = AggFunctions.group_by_agg(rows, ["g"], {"v": "min"})
        assert result[0]["v_min"] == 5.0
        result2 = AggFunctions.group_by_agg(rows, ["g"], {"v": "max"})
        assert result2[0]["v_max"] == 15.0


# ── Pivot Table Tests ────────────────────────────────────────────────

class TestPivotTable:
    def test_pivot_sum(self):
        rows = [
            {"region": "N", "product": "A", "sales": 10},
            {"region": "N", "product": "B", "sales": 20},
            {"region": "S", "product": "A", "sales": 30},
        ]
        result = PivotTable.pivot(rows, "region", "product", "sales", "sum")
        assert len(result) == 2
        n_row = [r for r in result if r["region"] == "N"][0]
        assert n_row["A"] == 10
        assert n_row["B"] == 20

    def test_pivot_count(self):
        rows = [
            {"r": "X", "p": "A", "v": 1},
            {"r": "X", "p": "A", "v": 2},
            {"r": "X", "p": "B", "v": 3},
        ]
        result = PivotTable.pivot(rows, "r", "p", "v", "count")
        x_row = result[0]
        assert x_row["A"] == 2
        assert x_row["B"] == 1

    def test_unpivot(self):
        rows = [{"id": 1, "q1": 10, "q2": 20, "q3": 30}]
        result = PivotTable.unpivot(rows, ["id"], ["q1", "q2", "q3"])
        assert len(result) == 3
        assert result[0]["variable"] == "q1"
        assert result[0]["value"] == 10


# ── CTE / Recursive Query Tests ─────────────────────────────────────

class TestCTERunner:
    def test_with_cte(self):
        base = lambda: [{"x": 1}, {"x": 2}, {"x": 3}]
        transform = lambda rows: [r for r in rows if r["x"] > 1]
        result = CTERunner.with_cte(base, transform)
        assert len(result) == 2

    def test_recursive(self, org_data):
        seed_fn = lambda: [r for r in org_data if r["manager_id"] is None]
        def step_fn(frontier):
            frontier_ids = {r["id"] for r in frontier}
            return [r for r in org_data if r["manager_id"] in frontier_ids
                    and r not in frontier]
        result = CTERunner.recursive(seed_fn, step_fn, max_depth=10)
        assert len(result) == len(org_data)

    def test_recursive_stops(self):
        seed_fn = lambda: [{"n": 1}]
        step_fn = lambda frontier: (
            [{"n": frontier[0]["n"] + 1}] if frontier[0]["n"] < 5 else []
        )
        result = CTERunner.recursive(seed_fn, step_fn)
        assert len(result) == 5


# ── Query Builder Tests ──────────────────────────────────────────────

class TestQueryBuilder:
    def test_simple_select(self):
        sql, _ = QueryBuilder("sales").select("*").build()
        assert "SELECT *" in sql
        assert "FROM sales" in sql

    def test_where(self):
        sql, params = (QueryBuilder("sales")
                       .select("id", "revenue")
                       .where("revenue > ?", 100)
                       .build())
        assert "WHERE revenue > ?" in sql
        assert params == [100]

    def test_group_by_having(self):
        sql, _ = (QueryBuilder("sales")
                  .select("region", "SUM(revenue) as total")
                  .group_by("region")
                  .having("total > 1000")
                  .build())
        assert "GROUP BY region" in sql
        assert "HAVING total > 1000" in sql

    def test_order_limit_offset(self):
        sql, _ = (QueryBuilder("sales")
                  .select("*")
                  .order_by("revenue", "DESC")
                  .limit(10)
                  .offset(5)
                  .build())
        assert "ORDER BY revenue DESC" in sql
        assert "LIMIT 10" in sql
        assert "OFFSET 5" in sql

    def test_join(self):
        sql, _ = (QueryBuilder("orders")
                  .select("orders.id", "customers.name")
                  .join("customers", "orders.cid = customers.id", "LEFT")
                  .build())
        assert "LEFT JOIN customers ON orders.cid = customers.id" in sql

    def test_with_cte_clause(self):
        sql, _ = (QueryBuilder("cte_table")
                  .with_cte("cte_table", "SELECT * FROM sales WHERE revenue > 100")
                  .select("*")
                  .build())
        assert "WITH cte_table AS" in sql

    def test_run_on_db(self, db):
        result = (QueryBuilder("sales")
                  .select("region", "COUNT(*) as cnt")
                  .group_by("region")
                  .order_by("cnt", "DESC")
                  .run(db))
        assert len(result) > 0
        assert "region" in result[0]
        assert "cnt" in result[0]


# ── Sample Data Tests ────────────────────────────────────────────────

class TestSampleData:
    def test_sales_data_count(self):
        rows = generate_sales_data(50)
        assert len(rows) == 50

    def test_sales_data_schema(self):
        rows = generate_sales_data(10)
        for key in ["id", "date", "region", "product", "quantity", "revenue", "cost"]:
            assert key in rows[0]

    def test_sales_data_deterministic(self):
        a = generate_sales_data(20, seed=1)
        b = generate_sales_data(20, seed=1)
        assert a == b

    def test_org_hierarchy(self):
        rows = generate_org_hierarchy(depth=3, branch=2)
        assert rows[0]["name"] == "CEO"
        assert rows[0]["manager_id"] is None
        assert len(rows) == 1 + 2 + 4  # 1 CEO + 2 L1 + 4 L2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
