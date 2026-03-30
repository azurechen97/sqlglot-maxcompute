# Changelog

## [0.2.0] - 2026-03-31

### Added

**Generator (MaxCompute SQL output)**

- `DATEADD` / `DATEADD` with negated delta for `DateSub` (e.g. BigQuery `DATE_SUB` → `DATEADD(dt, -3, 'DAY')`)
- `DATEDIFF` with optional unit argument
- `DATETRUNC` / `TRUNC_TIME` with full week-unit support (`'week'` → `'week(monday)'`)
- `DATEPART` via named `extract_sql` method
- `GETDATE()` for `CurrentTimestamp`, `NOW()` for `CurrentDatetime`
- `TOLOWER` / `TOUPPER` for `Lower` / `Upper`
- `FROM_JSON` for `ParseJSON`, `GET_USER_ID()` for `CurrentUser`, `TO_MILLIS` for `UnixMillis`
- `APPROX_DISTINCT`, `ARG_MAX`, `ARG_MIN`
- `WM_CONCAT(sep, col)` via named `groupconcat_sql` method (with default separator guard)
- `TO_CHAR` via named `tochar_sql` method
- Type mapping: `VARCHAR` / `CHAR` / `TEXT` → `STRING`; `DATETIME` preserved

**Parser (read direction)**

- Statistical aggregates: `STDDEV_SAMP`, `COVAR_POP`, `COVAR_SAMP`, `CORR`, `MEDIAN`, `PERCENTILE_APPROX`, `BITWISE_AND_AGG`, `BITWISE_OR_AGG`, `BITWISE_XOR_AGG`
- `MAX_BY` / `MIN_BY` as aliases for `ARG_MAX` / `ARG_MIN`

### Fixed

- `TO_DATE` format string preserved as-is (Hive was converting Oracle-style `mm` → `%M`)
- `WEEKDAY` round-trips as `WEEKDAY(dt)` in MaxCompute output (other dialects still get the `(DAYOFWEEK + 5) % 7` arithmetic)
- `DATETRUNC` week-unit emits valid `'week(monday)'` string literal instead of bare `WEEK(MONDAY)`
- `groupconcat_sql` defaults separator to `','` when absent (prevents invalid `WM_CONCAT(col)`)
- Python >=3.9 compatibility (removed `ipykernel`, relaxed `pytest` floor)
- Added `sqlglot<31` upper bound to prevent unexpected breakage on major releases

### Infrastructure

- GitHub Actions CI across Python 3.9 / 3.10 / 3.11 / 3.12
- README with usage examples and function coverage table

## [0.1.0] - 2026-03-01

Initial release.

- MaxCompute dialect registered via Python entry points
- Parser: ~40 date/time, string, array, and map functions
- DDL: `LIFECYCLE`, `RANGE CLUSTERED BY`, `AUTO PARTITIONED BY`
