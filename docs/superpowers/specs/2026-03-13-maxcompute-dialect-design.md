# MaxCompute Dialect Design

## Part 1 — SQLGlot Custom Dialect Guide

### Key source files to read first

| File | What it teaches |
|---|---|
| `sqlglot/dialects/dialect.py` | Base `Dialect` class, all class-level property flags, helper builders (`build_formatted_time`, `build_timetostr_or_tochar`, `unit_to_str`, `rename_func`, etc.) |
| `sqlglot/dialects/hive.py` | Our base class — read the full Tokenizer, Parser.FUNCTIONS, Generator.TRANSFORMS, and all `_sql` methods |
| `sqlglot/dialects/spark.py` | Good reference for a Hive subclass that selectively overrides |
| `sqlglot/dialects/bigquery.py` | Best reference for a comprehensive, well-tested dialect with many custom generators |
| `sqlglot/expressions.py` | All `exp.*` AST node definitions — search for the node name to see its `arg_types` |
| `sqlglot/tokens.py` | `TokenType` enum — for adding keywords in Tokenizer |
| `sqlglot/helper.py` | `seq_get()` — safe positional arg extractor used in all FUNCTIONS lambdas |

---

### Dialect anatomy

```python
class MaxCompute(Hive):
    # ── 1. Class-level properties ──────────────────────────────────────────
    # Behavioral flags inherited or overridden here.
    # See: sqlglot/dialects/dialect.py → class Dialect (top of file, all flags)

    TIME_MAPPING = { "yyyy": "%Y", ... }   # dialect fmt → strftime
    DATE_FORMAT  = "'yyyy-mm-dd'"
    TIME_FORMAT  = "'yyyy-mm-dd hh:mi:ss'"

    class Tokenizer(Hive.Tokenizer):
        # ── 2. Tokenizer ───────────────────────────────────────────────────
        # Controls lexical analysis: what is a keyword, quote char, identifier char.
        # See: sqlglot/dialects/hive.py → class Tokenizer
        QUOTES    = ["'"]                          # which chars open string literals
        KEYWORDS  = { **Hive.Tokenizer.KEYWORDS, "LIFECYCLE": TokenType.PROPERTY, ... }

    class Parser(Hive.Parser):
        # ── 3. Parser ──────────────────────────────────────────────────────
        # Maps function names (uppercase) → exp.* node constructors.
        # See: sqlglot/dialects/hive.py → class Parser → FUNCTIONS
        FUNCTIONS = {
            **Hive.Parser.FUNCTIONS,
            "DATEADD": lambda args: exp.TsOrDsAdd(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
                unit=seq_get(args, 2),
            ),
        }
        # For DDL properties (CREATE TABLE ... LIFECYCLE 30):
        PROPERTY_PARSERS = {
            **Hive.Parser.PROPERTY_PARSERS,
            "LIFECYCLE": lambda self: self.expression(
                exp.LifecycleProperty, this=self._parse_number()
            ),
        }

    class Generator(Hive.Generator):
        # ── 4. Generator ───────────────────────────────────────────────────
        # Maps exp.* node types → MaxCompute SQL strings.
        # See: sqlglot/dialects/hive.py → class Generator → TRANSFORMS
        TYPE_MAPPING = { **Hive.Generator.TYPE_MAPPING, exp.DataType.Type.DATETIME: "DATETIME" }
        TRANSFORMS = {
            **Hive.Generator.TRANSFORMS,
            exp.TsOrDsAdd: lambda self, e: self.func("DATEADD", e.this, e.expression, unit_to_str(e)),
        }
        PROPERTIES_LOCATION = {
            **Hive.Generator.PROPERTIES_LOCATION,
            exp.LifecycleProperty: exp.Properties.Location.POST_EXPRESSION,
        }

        # For complex generation logic, define a method named {classname_lower}_sql:
        def extract_sql(self, expression: exp.Extract) -> str:
            unit = exp.Literal.string(expression.this.name)
            return self.func("DATEPART", expression.expression, unit)
```

### Key patterns

**`self.func(name, *args)`** — generates `NAME(arg1, arg2, ...)`, handles None args gracefully.
Reference: `sqlglot/generator.py` → `def func()`

**`unit_to_str(e)`** — extracts the unit from a date expression as a quoted string literal.
Reference: `sqlglot/dialects/dialect.py` → `def unit_to_str()`

**`seq_get(args, i)`** — safe `args[i]`, returns None if out of bounds.
Reference: `sqlglot/helper.py` → `def seq_get()`

**`rename_func(name)`** — shorthand for `lambda self, e: self.func(name, *e.args.values())`.
Reference: `sqlglot/dialects/dialect.py` → `def rename_func()`

**`build_formatted_time(exp_class, dialect)`** — builds a time-formatting expression with format conversion.
Reference: `sqlglot/dialects/dialect.py` → `def build_formatted_time()`

**`exp.DataType.Type`** — enum of all canonical type names.
Reference: `sqlglot/expressions.py` → `class DataType`

### Testing pattern

```python
import sqlglot

# Transpile from another dialect into MaxCompute
assert sqlglot.transpile(
    "SELECT CURRENT_TIMESTAMP()", read="hive", write="maxcompute"
)[0] == "SELECT GETDATE()"

# Parse MaxCompute SQL → check canonical AST node type
ast = sqlglot.parse_one("SELECT DATEADD(dt, 1, 'day')", read="maxcompute")
# navigate: ast.selects[0] should be exp.TsOrDsAdd

# Round-trip: MaxCompute → MaxCompute
assert sqlglot.transpile(
    "SELECT GETDATE()", read="maxcompute", write="maxcompute"
)[0] == "SELECT GETDATE()"
```

Hive's own tests are the best style reference:
`sqlglot/tests/dialects/test_hive.py` (in the sqlglot repo, not this .venv)

---

## Part 2 — MaxCompute vs Hive Differences Checklist

This checklist is scoped to what MaxCompute does **differently** from Hive.
Items that Hive already handles correctly are omitted — `MaxCompute(Hive)` inherits them for free.

Legend: **[P]** = Property · **[T]** = Tokenizer · **[Pa]** = Parser · **[G]** = Generator

---

### Dialect properties

| # | Component | Item | Notes |
|---|---|---|---|
| 1 | [T] | `QUOTES = ["'"]` | Hive allows both `'` and `"`. MaxCompute: single quotes only for strings. Verify in tokenizer. |
| 2 | [P] | `NORMALIZATION_STRATEGY` | Likely `CASE_INSENSITIVE` same as Hive — verify against MaxCompute docs |
| 3 | [P] | `ALIAS_POST_TABLESAMPLE` | Hive sets `True`. Check if MaxCompute matches. |
| 4 | [P] | `IDENTIFIERS_CAN_START_WITH_DIGIT` | Hive sets `True`. Verify MaxCompute allows this. |
| 5 | [P] | `SAFE_DIVISION` | Hive sets `True` (div by zero → NULL). Verify MaxCompute behavior. |

---

### Tokenizer keywords

| # | Component | Keyword | Notes |
|---|---|---|---|
| 6 | [T] | `LIFECYCLE` | Table property. Hive doesn't have it. Add with appropriate `TokenType`. |
| 7 | [T] | `EXPORT` | Already in current impl. |
| 8 | [T] | `OPTION` | Already in current impl. |

To find what `TokenType` to use: `sqlglot/tokens.py` → `class TokenType`

---

### Parser — date/time functions

All of these are MaxCompute-specific or have different signatures from Hive.
For each, look up the canonical `exp.*` node in `sqlglot/expressions.py`.

| # | MaxCompute function | Arg order | Maps to | Hive equivalent | Notes |
|---|---|---|---|---|---|
| 9 | `DATEADD(date, delta, unit)` | date,delta,unit | `exp.TsOrDsAdd` | `DATE_ADD(date, n)` (day only, no unit) | Unit as 3rd arg |
| 10 | `DATE_ADD(date, delta)` | date,delta | `exp.DateAdd` | same name | Already in Hive — check if inherited correctly |
| 11 | `DATEDIFF(unit, date1, date2)` | **unit first!** | `exp.DateDiff` | `DATEDIFF(d1,d2)` — **no unit arg** | Arg order difference is critical |
| 12 | `DATE_SUB(date, delta)` | date,delta | `exp.DateSub` | same name | Check inheritance |
| 13 | `DATEPART(date, unit_str)` | date,unit | `exp.Extract` | `EXTRACT(unit FROM date)` | Unit is a string literal, not keyword |
| 14 | `DATETRUNC(date, unit)` | date,unit | `exp.TimestampTrunc` | `TRUNC(date, unit)` | Different function name |
| 15 | `GETDATE()` | — | `exp.CurrentTimestamp` | `CURRENT_TIMESTAMP` | Zero-arg function |
| 16 | `TO_CHAR(date, fmt)` | date,fmt | `exp.TimeToStr` | `DATE_FORMAT(date, fmt)` | Format code system also differs (see TIME_MAPPING) |
| 17 | `TO_DATE(str, fmt)` | str,fmt | `exp.StrToDate` | `TO_DATE(str)` — **no fmt** | Hive version has no format arg |
| 18 | `FROM_UNIXTIME(unix, fmt)` | unix,fmt | `exp.UnixToTime` | same — likely inherited | Verify format arg handling |
| 19 | `UNIX_TIMESTAMP(str, fmt)` | str,fmt | `exp.StrToUnix` | same — likely inherited | Check |
| 20 | `ADD_MONTHS(date, n)` | date,n | `exp.AddMonths` | same — likely inherited | Verify |
| 21 | `MONTHS_BETWEEN(d1, d2)` | d1,d2 | `exp.MonthsBetween` | same — likely inherited | Verify |
| 22 | `LAST_DAY(date)` | date | `exp.LastDay` | same — likely inherited | Verify |
| 23 | `NEXT_DAY(date, weekday)` | date,weekday | `exp.NextDay` | same | Verify |
| 24 | `WEEKOFYEAR(date)` | date | `exp.WeekOfYear` | same — likely inherited | Verify |

---

### Parser — string functions

| # | MaxCompute function | Arg order | Maps to | Notes |
|---|---|---|---|---|
| 25 | `TOLOWER(str)` | str | `exp.Lower` | Alias. Hive uses `LOWER` directly. |
| 26 | `TOUPPER(str)` | str | `exp.Upper` | Alias. Hive uses `UPPER` directly. |
| 27 | `WM_CONCAT(sep, col)` | **sep first!** | `exp.GroupConcat` | Hive has no direct equivalent. Arg order is reversed from standard `GROUP_CONCAT(col, sep)`. |
| 28 | `FROM_JSON(str, schema)` | str,schema | `exp.ParseJSON` | Verify Hive's `FROM_JSON` inheritance works correctly. |
| 29 | `REGEXP_SUBSTR(str, pattern)` | str,pattern | `exp.RegexpExtract` | MaxCompute alias for regexp extract. |
| 30 | `GET_JSON_OBJECT(str, path)` | str,path | `exp.JSONExtract` | Likely inherited from Hive — verify. |

---

### Parser — aggregate functions

| # | MaxCompute function | Maps to | Notes |
|---|---|---|---|
| 31 | `COUNT_IF(cond)` | `exp.CountIf` | Hive doesn't have this. |
| 32 | `APPROX_DISTINCT(col)` | `exp.ApproxDistinct` | Check Hive inheritance. |
| 33 | `ARG_MAX(col, measure)` | `exp.ArgMax` | Check Hive inheritance. |
| 34 | `ARG_MIN(col, measure)` | `exp.ArgMin` | Check Hive inheritance. |
| 35 | `COLLECT_LIST` | `exp.ArrayAgg` | Should be inherited from Hive. |
| 36 | `COLLECT_SET` | `exp.ArrayUniqueAgg` | Should be inherited from Hive. |

---

### Parser — other functions

| # | MaxCompute function | Maps to | Notes |
|---|---|---|---|
| 37 | `GET_USER_ID()` | `exp.CurrentUser` | No Hive equivalent. |
| 38 | `SLICE(arr, start, len)` | `exp.ArraySlice` | No Hive equivalent. |

---

### Parser — DDL

| # | Item | Notes |
|---|---|---|
| 39 | `LIFECYCLE n` in `CREATE TABLE` | Needs `PROPERTY_PARSERS` entry producing `exp.LifecycleProperty`. Check `sqlglot/expressions.py` for `LifecycleProperty` definition — it may already exist. |

---

### Generator — date/time transforms

For each entry, look at `sqlglot/dialects/hive.py → Generator.TRANSFORMS` to see what Hive currently generates, then override where MaxCompute differs.

| # | Canonical expression | MaxCompute output | Notes |
|---|---|---|---|
| 40 | `exp.TsOrDsAdd` | `DATEADD(date, delta, unit)` | Override Hive |
| 41 | `exp.DateAdd` | `DATEADD(date, delta, 'day')` or keep `DATE_ADD` | Decide which |
| 42 | `exp.DateSub` | `DATEADD(date, -delta, unit)` | Override Hive |
| 43 | `exp.TsOrDsDiff` | `DATEDIFF(unit, d1, d2)` | Unit is first arg |
| 44 | `exp.DateDiff` | `DATEDIFF(unit, d1, d2)` | Override Hive |
| 45 | `exp.TimestampTrunc` | `DATETRUNC(date, unit)` | Override Hive's `TRUNC` |
| 46 | `exp.Extract` | `DATEPART(date, 'unit')` | Custom `_sql` method needed — unit becomes a string literal |
| 47 | `exp.CurrentTimestamp` | `GETDATE()` | Custom `currenttimestamp_sql` method |
| 48 | `exp.TimeToStr` | `TO_CHAR(date, fmt)` | Custom `timetostr_sql` method; also handle `TimeStrToTime` wrapping |

---

### Generator — string transforms

| # | Canonical expression | MaxCompute output | Notes |
|---|---|---|---|
| 49 | `exp.Lower` | `TOLOWER(str)` | Override Hive's `LOWER` |
| 50 | `exp.Upper` | `TOUPPER(str)` | Override Hive's `UPPER` |
| 51 | `exp.GroupConcat` | `WM_CONCAT(sep, col)` | Custom `groupconcat_sql` method; arg order reversed |
| 52 | `exp.ParseJSON` | `FROM_JSON(str, schema)` | Check Hive's output and override if needed |

---

### Generator — type mapping

| # | Canonical type | MaxCompute output | Notes |
|---|---|---|---|
| 53 | `exp.DataType.Type.DATETIME` | `DATETIME` | Hive maps DATETIME → TIMESTAMP; MaxCompute has a native DATETIME type |
| 54 | `exp.DataType.Type.VARCHAR` | `STRING` | MaxCompute prefers STRING |
| 55 | `exp.DataType.Type.CHAR` | `STRING` | Same |
| 56 | `exp.DataType.Type.TEXT` | `STRING` | Same |

Check `sqlglot/dialects/hive.py → Generator.TYPE_MAPPING` for current Hive mappings to understand what's already handled.

---

### Generator — DDL

| # | Item | Notes |
|---|---|---|
| 57 | `exp.LifecycleProperty` | Output `LIFECYCLE n`. Set `PROPERTIES_LOCATION` to `POST_EXPRESSION`. Reference: Hive's `alterset_sql` or BigQuery's custom property generators. |

---

### TIME_MAPPING

MaxCompute uses Oracle-style format codes. Hive uses `%`-style (strftime). The `TIME_MAPPING` dict on the dialect class controls bidirectional translation via `format_time()`.

| MaxCompute format code | strftime equivalent | Notes |
|---|---|---|
| `yyyy` | `%Y` | 4-digit year |
| `yy` | `%y` | 2-digit year |
| `mm` | `%m` | Month |
| `dd` | `%d` | Day |
| `hh` | `%H` | Hour (24h) |
| `mi` | `%M` | Minute |
| `ss` | `%S` | Second |
| `ff3` | `%f` | Millisecond |

Reference: `sqlglot/dialects/dialect.py` → `format_time()`, `build_formatted_time()`

---

### Test checklist (per category)

For each category, write three test types:

```
[Pa] Parse:      sqlglot.parse_one("...", read="maxcompute")  → check AST node type
[G]  Transpile:  sqlglot.transpile("...", read="hive", write="maxcompute")  → assert output
[RT] Round-trip: sqlglot.transpile("...", read="maxcompute", write="maxcompute")  → assert unchanged
```

| # | Category | Tests needed |
|---|---|---|
| T1 | Date functions | [Pa]+[G]+[RT] for each of items 9–24 |
| T2 | String functions | [Pa]+[G]+[RT] for items 25–30 |
| T3 | Aggregate functions | [Pa]+[G] for items 31–36 |
| T4 | Other functions | [Pa]+[G] for items 37–38 |
| T5 | DDL (LIFECYCLE) | [Pa]+[G] for item 39/57 |
| T6 | Type mapping | [G] for items 53–56 |
| T7 | Time format codes | Transpile `TO_CHAR(dt, 'yyyy-mm-dd')` ↔ `DATE_FORMAT(dt, '%Y-%m-%d')` |

---

### Where to look in sqlglot for each `exp.*` node

When you need to implement a Parser entry and aren't sure which `exp.*` node to use, or what its `arg_types` are:

1. Search `sqlglot/expressions.py` for the class name (e.g. `class TsOrDsAdd`)
2. Check `arg_types` dict — tells you what named args the node expects
3. Check existing uses in `sqlglot/dialects/` — search for `exp.TsOrDsAdd` across dialect files

For Generator, after choosing the node, search `Generator.TRANSFORMS` in `hive.py` to see if Hive already generates something for it — and whether you need to override.
