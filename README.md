# sqlglot-maxcompute

A [SQLGlot](https://github.com/tobymao/sqlglot) dialect plugin for [Alibaba Cloud MaxCompute](https://www.alibabacloud.com/product/maxcompute) (formerly ODPS).

Registers the `maxcompute` dialect via Python entry points so that SQLGlot can parse and generate MaxCompute SQL.

## Installation

```bash
pip install sqlglot-maxcompute
```

Requires Python ≥ 3.9 and SQLGlot ≥ 29.

## Usage

```python
import sqlglot

# Parse MaxCompute SQL
ast = sqlglot.parse_one("SELECT DATEADD(dt, 1, 'DAY')", read="maxcompute")

# Transpile from another dialect to MaxCompute
sqlglot.transpile("SELECT DATE_ADD(dt, 1)", read="spark", write="maxcompute")
# ["SELECT DATEADD(dt, 1, 'DAY')"]

# Transpile from MaxCompute to another dialect
sqlglot.transpile("SELECT DATETRUNC(dt, 'MONTH')", read="maxcompute", write="spark")
# ["SELECT TRUNC(dt, 'MONTH')"]

# TO_DATE return type depends on args:
#   without format → DATE    (exp.TsOrDsToDate)
#   with format    → DATETIME (exp.StrToTime)
sqlglot.transpile("TO_DATE('20240101', 'yyyymmdd')", read="maxcompute", write="spark")
# ["TO_TIMESTAMP('20240101', 'yyyymmdd')"]

# Round-trip MaxCompute DDL
sqlglot.transpile(
    "CREATE TABLE t (id BIGINT) LIFECYCLE 30",
    read="maxcompute",
    write="maxcompute",
)
# ["CREATE TABLE t (id BIGINT) LIFECYCLE 30"]
```

## What's supported

### Parser — MaxCompute → canonical AST

| Category | Functions |
|---|---|
| Date arithmetic | `DATEADD`, `DATE_SUB`, `DATEDIFF`, `ADD_MONTHS`, `MONTHS_BETWEEN` |
| Date extraction | `DATEPART`, `DATETRUNC`, `TRUNC_TIME`, `DAY`, `MONTH`, `YEAR`, `HOUR`, `MINUTE`, `SECOND`, `QUARTER`, `DAYOFMONTH`, `DAYOFWEEK`, `DAYOFYEAR`, `WEEKDAY`, `WEEKOFYEAR` |
| Date conversion | `TO_DATE`, `DATE_FORMAT`, `TO_CHAR`, `FROM_UNIXTIME`, `FROM_UTC_TIMESTAMP`, `TO_MILLIS`, `ISDATE` |
| Current date/time | `GETDATE`, `NOW`, `CURRENT_TIMESTAMP`, `CURRENT_TIMEZONE` |
| Last/next day | `LAST_DAY`, `LASTDAY`, `NEXT_DAY` |
| String | `TOLOWER`, `TOUPPER`, `REGEXP_COUNT`, `SPLIT_PART`, `SUBSTR` |
| Aggregate | `WM_CONCAT`, `COUNT_IF`, `ARG_MAX`, `ARG_MIN`, `MAX_BY`, `MIN_BY`, `ANY_VALUE`, `APPROX_DISTINCT`, `STDDEV_SAMP`, `COVAR_POP`, `COVAR_SAMP`, `CORR`, `MEDIAN`, `PERCENTILE_APPROX`, `BITWISE_AND_AGG`, `BITWISE_OR_AGG`, `BITWISE_XOR_AGG` |
| Array | `ALL_MATCH`, `ANY_MATCH`, `ARRAY_SORT`, `ARRAY_DISTINCT`, `ARRAY_EXCEPT`, `ARRAY_JOIN`, `ARRAY_MAX`, `ARRAY_MIN`, `ARRAYS_OVERLAP`, `ARRAYS_ZIP`, `ARRAY_INTERSECT`, `ARRAY_POSITION`, `ARRAY_REMOVE`, `ARRAY_CONTAINS`, `SLICE` |
| Map | `MAP_CONCAT`, `MAP_FROM_ENTRIES` |
| JSON / misc | `FROM_JSON`, `GET_JSON_OBJECT`, `JSON_TUPLE`, `GET_USER_ID`, `REGEXP_SUBSTR`, `TO_MILLIS`, `ISDATE` |

Functions not listed are handled via Hive inheritance and work without explicit mapping (e.g. `SPLIT`, `REGEXP_EXTRACT`, `COLLECT_LIST`, `PERCENTILE`, all math/trig functions, window functions).

### Generator — canonical AST → MaxCompute SQL

Explicit transforms on top of Hive:

| Expression | MaxCompute output | Note |
|---|---|---|
| `DATEADD` / `DATE_SUB` | `DATEADD(dt, ±n, 'UNIT')` | Correct negation for `DATE_SUB` |
| `DATEDIFF` | `DATEDIFF(dt1, dt2[, unit])` | |
| `DATETRUNC` | `DATETRUNC(dt, 'unit')` | Week units: `'week(monday)'` etc. |
| `DATEPART` | `DATEPART(dt, 'UNIT')` | |
| `TO_DATE(str, fmt)` | `TO_DATE(str, fmt)` | Maps to `exp.StrToTime` (DATETIME) |
| `TO_DATE(str)` | `TO_DATE(str)` | Maps to `exp.TsOrDsToDate` (DATE) |
| `CurrentTimestamp` | `GETDATE()` | Covers `GETDATE`, `NOW`, `CURRENT_TIMESTAMP` |
| `CurrentDatetime` | `NOW()` | For BigQuery-origin `CURRENT_DATETIME` |
| `SPACE(n)` | `SPACE(n)` | Hive emits `REPEAT(' ', n)` |
| `VAR_POP(x)` | `VAR_POP(x)` | Hive emits `VARIANCE_POP` |
| `VAR_SAMP(x)` | `VAR_SAMP(x)` | Hive emits `VARIANCE` |
| `INSTR(str, sub)` | `INSTR(str, sub)` | Hive emits `LOCATE(sub, str)` |
| `SUBSTR(str, pos, len)` | `SUBSTR(...)` | Hive emits `SUBSTRING` |
| Type: `VARCHAR`/`CHAR`/`TEXT` | `STRING` | |
| Type: `DATETIME` | `DATETIME` | |

### DDL

- `LIFECYCLE n` — table retention in days
- `RANGE CLUSTERED BY (cols) [SORTED BY (cols)] INTO n BUCKETS`
- `AUTO PARTITIONED BY (TRUNC_TIME(col, 'unit') [AS alias])`
- `TBLPROPERTIES ('key'='value')` — coexists correctly with `LIFECYCLE`
- `COMMENT` on columns and tables

## Development

```bash
uv sync            # install dependencies
uv run pytest      # run all tests
```

## License

MIT
