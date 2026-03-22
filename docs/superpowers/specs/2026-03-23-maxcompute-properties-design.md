# MaxCompute Properties: Parser & Generator Design

**Date:** 2026-03-23
**Branch:** feat/property-parsers
**Scope:** Three property-related fixes/features in `src/sqlglot_maxcompute/maxcompute.py`

---

## Background

The MaxCompute dialect inherits from Hive. `CREATE TABLE` in MaxCompute has several property/clause forms that differ from Hive:

1. `LIFECYCLE <days>` — standalone bare keyword clause (MaxCompute-only)
2. `TBLPROPERTIES ('key'='value', ...)` — inherited from Hive (transactional, Delta Table config, etc.)
3. `RANGE CLUSTERED BY (cols) [SORTED BY (cols)] [INTO N BUCKETS]` — range clustering (MaxCompute-only; Hive has hash-only `CLUSTERED BY`)
4. `AUTO PARTITIONED BY (TRUNC_TIME(col, unit) [AS name])` — expression-based auto partition (MaxCompute-only; conceptually matches BigQuery's `PARTITION BY DATE_TRUNC(...)`)

### Plugin contract constraints (from CLAUDE.md)

- No custom `exp.Property` subclasses
- No new `exp.*` expression classes
- No monkey-patching sqlglot internals outside the `MaxCompute` class hierarchy

---

## Problem 1: TBLPROPERTIES + LIFECYCLE bug

### Root cause

The current implementation overrides `PROPERTIES_LOCATION[exp.Property] = POST_SCHEMA`, which moves **all** `exp.Property` instances (including string-keyed TBLPROPERTIES entries like `'transactional'='true'`) out of Hive's `WITH_PROPERTIES_PREFIX` (TBLPROPERTIES) wrapper. Result:

```sql
-- Expected
CREATE TABLE t (a INT) TBLPROPERTIES ('transactional'='true') LIFECYCLE 7
-- Actual (broken)
CREATE TABLE t (a INT) 'transactional'='true' LIFECYCLE 7
```

### Fix

Remove `exp.Property` from `PROPERTIES_LOCATION` and `TRANSFORMS` overrides. Add `properties_sql` override that intercepts before location-dispatch:

- Extracts Var-keyed properties (e.g. `exp.Property(this=Var("LIFECYCLE"), value=7)`)
- Calls `super().properties_sql()` on the remaining properties (Hive handles TBLPROPERTIES correctly)
- Appends bare `KEY value` pairs after the Hive-rendered output

```python
def properties_sql(self, expression: exp.Properties) -> str:
    var_keyed = [
        p for p in expression.expressions
        if isinstance(p, exp.Property) and isinstance(p.this, exp.Var)
    ]
    other = [p for p in expression.expressions if p not in var_keyed]
    other_node = exp.Properties(expressions=other)
    other_node.parent = expression.parent
    base_sql = super().properties_sql(other_node) if other else ""
    bare_sql = " ".join(f"{p.name} {self.sql(p, 'value')}" for p in var_keyed)
    return f"{base_sql} {bare_sql}".strip() if (base_sql and bare_sql) else base_sql or bare_sql
```

**No parser changes needed** — `LIFECYCLE` parsing is already correct.

---

## Problem 2: RANGE CLUSTERED BY

### Background

MaxCompute supports both hash clustering (`CLUSTERED BY`) and range clustering (`RANGE CLUSTERED BY`). The base parser's `PROPERTY_PARSERS["RANGE"]` maps to `_parse_dict_range`, causing a parse error on MaxCompute's RANGE CLUSTERED BY syntax.

`RANGE CLUSTERED BY` has no equivalent in Hive, Spark, or other supported dialects → **round-trip only**.

### AST representation

Reuse `exp.ClusteredByProperty` with an undeclared `range=True` arg stored in `expression.args`. Undeclared args survive `copy()`/`deepcopy()` in sqlglot's `Expression` base class. Access via `expression.args.get("range")`.

### Parser

Override `PROPERTY_PARSERS["RANGE"]` to branch on what follows:

```python
"RANGE": lambda self: (
    self._parse_range_clustered_by()
    if self._match_text_seq("CLUSTERED", "BY")
    else self._parse_dict_range(this="RANGE")
),
```

Add `_parse_range_clustered_by` instance method on `MaxCompute.Parser`:
- Parses `(col_list) [SORTED BY (sorted_cols)] [INTO N BUCKETS]`
- Builds `exp.ClusteredByProperty(expressions=..., sorted_by=..., buckets=...)`
- Sets `node.args["range"] = True` before returning

The parsing logic mirrors what the base parser does for `CLUSTERED BY` — look at `_parse_clustered_to_property` in the installed sqlglot version and replicate/call it.

### Generator

Override `clusteredbyproperty_sql` to prepend `RANGE ` when the range flag is set:

```python
def clusteredbyproperty_sql(self, expression: exp.ClusteredByProperty) -> str:
    sql = super().clusteredbyproperty_sql(expression)
    return f"RANGE {sql}" if expression.args.get("range") else sql
```

---

## Problem 3: AUTO PARTITIONED BY

### Background

MaxCompute syntax:
```sql
AUTO PARTITIONED BY (TRUNC_TIME(col, 'unit') [AS partition_col_name])
```

BigQuery uses an identical concept:
```sql
PARTITION BY DATE_TRUNC(col, MONTH)
```

Both map to the same AST node: `PartitionedByProperty(this=DateTrunc(...))`. The `TRUNC_TIME` function already maps to `exp.DateTrunc`/`exp.TimestampTrunc` via the existing `FUNCTIONS` entry, so no new function mapping is needed.

### AST representation

- Without `AS`: `PartitionedByProperty(this=DateTrunc(unit=..., this=Column(...)))`
- With `AS name`: `PartitionedByProperty(this=Alias(this=DateTrunc(...), alias=Identifier("name")))`

### Parser

Add `PROPERTY_PARSERS["AUTO"]` that matches `PARTITIONED BY`, parses the inner expression (resolves via FUNCTIONS to `DateTrunc`), handles optional `AS name`, and builds `PartitionedByProperty`:

```python
"AUTO": lambda self: self._parse_auto_partition(),
```

Add `_parse_auto_partition` instance method on `MaxCompute.Parser`:
1. Match text `PARTITIONED` and `BY`
2. Match `(`
3. Parse inner expression — `TRUNC_TIME(...)` resolves to `DateTrunc` via existing FUNCTIONS
4. If `AS` follows, parse identifier as alias; wrap expression in `exp.Alias`
5. Match `)`
6. Return `self.expression(exp.PartitionedByProperty, this=expr)`

### Generator

Override `PartitionedByProperty` in TRANSFORMS. Detect `DateTrunc`/`TimestampTrunc`/`DatetimeTrunc` (possibly wrapped in `Alias`) as the `this` child to identify auto-partition nodes:

```python
exp.PartitionedByProperty: lambda self, e: (
    self._auto_partitioned_by_sql(e)
    if isinstance(e.this, (exp.DateTrunc, exp.TimestampTrunc, exp.DatetimeTrunc, exp.Alias))
    else Hive.Generator.TRANSFORMS[exp.PartitionedByProperty](self, e)
),
```

Add `_auto_partitioned_by_sql` method:
1. Unwrap `Alias` if present, capture alias name
2. For the inner `DateTrunc`/`TimestampTrunc`: render as `TRUNC_TIME(col, 'unit')`
3. Append `AS name` if alias was present
4. Wrap in `AUTO PARTITIONED BY (...)`

### Cross-dialect behavior

| Source → Target | Behavior |
|---|---|
| MaxCompute → MaxCompute | Round-trip: `AUTO PARTITIONED BY (TRUNC_TIME(dt, 'month'))` |
| BigQuery → MaxCompute | `PARTITION BY DATE_TRUNC(dt, MONTH)` → `AUTO PARTITIONED BY (TRUNC_TIME(dt, 'month'))` |
| MaxCompute → BigQuery | `AUTO PARTITIONED BY (TRUNC_TIME(dt, 'month'))` → `PARTITION BY DATE_TRUNC(dt, MONTH)` |
| MaxCompute → Hive/Spark | `PartitionedByProperty(this=DateTrunc)` → `PARTITIONED BY` (incomplete — acceptable, no equivalent exists) |

---

## Tests

All tests go in `tests/test_maxcompute.py` using the existing `Validator` base class.

### TBLPROPERTIES fix

```python
# Round-trip: TBLPROPERTIES alone
validate_identity("CREATE TABLE t (a INT) TBLPROPERTIES ('transactional'='true')")
# Round-trip: TBLPROPERTIES + LIFECYCLE together
validate_identity("CREATE TABLE t (a INT) TBLPROPERTIES ('transactional'='true') LIFECYCLE 7")
# LIFECYCLE alone still works
validate_identity("CREATE TABLE t (a INT) LIFECYCLE 30")
```

### RANGE CLUSTERED BY

```python
# Parse: check ClusteredByProperty with range flag
e = parse_one("CREATE TABLE t (a INT) RANGE CLUSTERED BY (a) SORTED BY (a) INTO 1024 BUCKETS", read="maxcompute")
prop = e.args["properties"].expressions[0]
assertIsInstance(prop, exp.ClusteredByProperty)
assertTrue(prop.args.get("range"))

# Round-trip
validate_identity("CREATE TABLE t (a INT) RANGE CLUSTERED BY (a) SORTED BY (a) INTO 1024 BUCKETS")
# Without SORTED BY (buckets optional per docs)
validate_identity("CREATE TABLE t (a INT) RANGE CLUSTERED BY (a) INTO 512 BUCKETS")
```

### AUTO PARTITIONED BY

```python
# Parse: PartitionedByProperty with DateTrunc child
e = parse_one("CREATE TABLE t (a INT, dt DATETIME) AUTO PARTITIONED BY (TRUNC_TIME(dt, 'month'))", read="maxcompute")
prop = next(p for p in e.args["properties"].expressions if isinstance(p, exp.PartitionedByProperty))
assertIsInstance(prop.this, (exp.DateTrunc, exp.TimestampTrunc, exp.DatetimeTrunc))

# Round-trip without AS
validate_identity("CREATE TABLE t (a INT, dt DATETIME) AUTO PARTITIONED BY (TRUNC_TIME(dt, 'month'))")
# Round-trip with AS
validate_identity("CREATE TABLE t (a INT, dt DATETIME) AUTO PARTITIONED BY (TRUNC_TIME(dt, 'month') AS pt)")

# BigQuery → MaxCompute
validate_all(
    "CREATE TABLE t (a INT64) PARTITION BY DATE_TRUNC(dt, MONTH)",
    read="bigquery",
    write={"maxcompute": "CREATE TABLE t (a BIGINT) AUTO PARTITIONED BY (TRUNC_TIME(dt, 'month'))"},
)
```

---

## Files changed

- `src/sqlglot_maxcompute/maxcompute.py` — all changes
- `tests/test_maxcompute.py` — new test methods in existing test class
