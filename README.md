# sqlglot-maxcompute

A [SQLGlot](https://github.com/tobymao/sqlglot) dialect plugin for [Alibaba Cloud MaxCompute](https://www.alibabacloud.com/product/maxcompute) (formerly ODPS).

Registers the `maxcompute` dialect via Python entry points so that SQLGlot can parse and generate MaxCompute SQL.

## Installation

```bash
pip install sqlglot-maxcompute
```

## Usage

```python
import sqlglot

# Parse MaxCompute SQL
ast = sqlglot.parse_one("SELECT DATEADD(dt, 1, 'DAY')", read="maxcompute")

# Transpile from another dialect to MaxCompute
sqlglot.transpile(
    "SELECT DATE_ADD(dt, 1)",
    read="spark",
    write="maxcompute",
)
# ["SELECT DATEADD(dt, 1, 'DAY')"]

# Transpile from MaxCompute to another dialect
sqlglot.transpile(
    "SELECT DATETRUNC(dt, 'MONTH')",
    read="maxcompute",
    write="spark",
)
# ["SELECT TRUNC(dt, 'MONTH')"]

# Round-trip: parse and regenerate MaxCompute SQL
sqlglot.transpile(
    "CREATE TABLE t (id INT) LIFECYCLE 30",
    read="maxcompute",
    write="maxcompute",
)
# ["CREATE TABLE t (id INT) LIFECYCLE 30"]
```

## What's implemented

### Parser (MaxCompute → canonical AST)

| Category | Functions |
|---|---|
| Date arithmetic | `DATEADD`, `DATEDIFF`, `ADD_MONTHS`, `MONTHS_BETWEEN` |
| Date extraction | `DATEPART`, `DATETRUNC`, `TRUNC_TIME`, `DAYOFMONTH`, `DAYOFWEEK`, `DAYOFYEAR`, `HOUR`, `MINUTE`, `SECOND`, `QUARTER`, `WEEKDAY`, `WEEKOFYEAR` |
| Date conversion | `DATE_FORMAT`, `TO_CHAR`, `TO_DATE`, `FROM_UNIXTIME`, `GETDATE`, `NOW`, `CURRENT_TIMESTAMP`, `CURRENT_TIMEZONE`, `FROM_UTC_TIMESTAMP` |
| Last/next day | `LAST_DAY`, `LASTDAY`, `NEXT_DAY` |
| String | `TOLOWER`, `TOUPPER`, `REGEXP_COUNT`, `SPLIT_PART` |
| Aggregate | `WM_CONCAT`, `COUNT_IF`, `ARG_MAX`, `ARG_MIN`, `ANY_VALUE`, `APPROX_DISTINCT`, `STDDEV_SAMP`, `COVAR_POP`, `COVAR_SAMP`, `CORR`, `MEDIAN`, `PERCENTILE_APPROX`, `BITWISE_AND_AGG`, `BITWISE_OR_AGG`, `BITWISE_XOR_AGG` |
| Array | `ALL_MATCH`, `ANY_MATCH`, `ARRAY_SORT`, `ARRAY_DISTINCT`, `ARRAY_EXCEPT`, `ARRAY_JOIN`, `ARRAY_MAX`, `ARRAY_MIN`, `ARRAYS_OVERLAP`, `ARRAYS_ZIP`, `ARRAY_INTERSECT`, `ARRAY_POSITION`, `ARRAY_REMOVE`, `ARRAY_CONTAINS` |
| Map | `MAP_CONCAT`, `MAP_FROM_ENTRIES` |
| JSON / misc | `FROM_JSON`, `GET_USER_ID`, `REGEXP_SUBSTR`, `SLICE`, `TO_MILLIS`, `ISDATE` |

### Generator (canonical AST → MaxCompute SQL)

- Date/time: `DATEADD`, `DATEDIFF`, `DATETRUNC`, `DATEPART`, `GETDATE()`, `NOW()`
- String: `TOLOWER`, `TOUPPER`
- Aggregate: `WM_CONCAT`, `ARG_MAX`, `ARG_MIN`, `APPROX_DISTINCT`
- JSON/misc: `FROM_JSON`, `GET_USER_ID()`, `TO_MILLIS`, `TO_CHAR`
- Type mapping: `VARCHAR`/`CHAR`/`TEXT` → `STRING`, `DATETIME` preserved

### DDL

- `LIFECYCLE n` — table retention in days
- `RANGE CLUSTERED BY (cols) [SORTED BY (cols)] INTO n BUCKETS`
- `AUTO PARTITIONED BY (TRUNC_TIME(col, 'unit') [AS alias])`
- `TBLPROPERTIES ('key'='value')` coexists correctly with `LIFECYCLE`

## Development

```bash
# Install dependencies
uv sync

# Run tests
uv run pytest

# Run a single test
uv run pytest tests/test_maxcompute.py::TestMaxCompute::test_dateadd_roundtrip
```

## License

MIT
