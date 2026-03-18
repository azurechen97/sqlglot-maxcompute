# MaxCompute Dialect Design

## Part 1 — SQLGlot Custom Dialect Guide

> For deeper background, read the official docs in `local/posts/`:
> - `onboarding.md` — full architecture walkthrough (Tokenizer → Parser → Generator → Dialects)
> - `ast_primer.md` — how AST nodes work, how to traverse and mutate them

---

### How transpilation works (the big picture)

SQLGlot transpilation is a three-step pipeline:

```
SQL string  →[Tokenizer]→  token list  →[Parser]→  AST  →[Generator]→  SQL string
```

1. **Tokenizer** splits the raw string into typed tokens (`SELECT`, `b`, `FROM`, `=`, `1`, …).
2. **Parser** reads those tokens and builds an **Abstract Syntax Tree (AST)** — a dialect-neutral tree of `exp.*` Python objects that captures the *meaning*, not the syntax.
3. **Generator** walks the AST and emits SQL in the target dialect.

Because the AST is dialect-neutral, transpiling from Hive to MaxCompute means:
- Parse with the Hive Tokenizer + Parser → AST
- Generate with the MaxCompute Generator → MaxCompute SQL

**You only need to teach your dialect how to read (Parser) and how to write (Generator).**

---

### Understanding AST nodes

Every node in the AST is a subclass of `exp.Expression`. Each class has an `arg_types` dict that declares its named children:

```python
# From sqlglot/expressions.py
class TsOrDsAdd(Expression):
    arg_types = {"this": True, "expression": True, "unit": False, "zone": False}
    #             ↑ the date    ↑ the delta           ↑ e.g. "day"
```

The two most important children are always:
- `this` — the primary child (access via `e.this`)
- `expression` — the secondary child (access via `e.expression`)

**Debugging trick**: when you're not sure what AST node a SQL fragment produces, parse it and `repr()` it:

```python
from sqlglot import parse_one
repr(parse_one("SELECT CURRENT_TIMESTAMP()", dialect="hive"))
# Select(expressions=[CurrentTimestamp()])
```

This tells you the exact node class to target in your Generator.

---

### Dialect anatomy

A dialect is a class with three inner classes. Since MaxCompute is based on Hive, we subclass `Hive` and only override what differs:

```python
from sqlglot.dialects.hive import Hive
from sqlglot import exp
from sqlglot.tokens import TokenType
from sqlglot.helper import seq_get
from sqlglot.dialects.dialect import unit_to_str

class MaxCompute(Hive):
    # Class-level: behavioral flags and format mappings
    # Reference: sqlglot/dialects/dialect.py → class Dialect
    TIME_MAPPING = {"yyyy": "%Y", "mm": "%m", ...}  # MaxCompute fmt → strftime
    DATE_FORMAT  = "'yyyy-mm-dd'"
    TIME_FORMAT  = "'yyyy-mm-dd hh:mi:ss'"

    class Tokenizer(Hive.Tokenizer):
        # Teaches the lexer what counts as a string, identifier, or keyword.
        # Reference: sqlglot/dialects/hive.py → Hive.Tokenizer
        QUOTES   = ["'"]                    # MaxCompute: single quotes only
        KEYWORDS = {
            **Hive.Tokenizer.KEYWORDS,
            "LIFECYCLE": TokenType.PROPERTY,  # new keyword not in Hive
        }

    class Parser(Hive.Parser):
        # Teaches the parser how to turn MaxCompute function calls into AST nodes.
        # Reference: sqlglot/dialects/hive.py → Hive.Parser.FUNCTIONS
        FUNCTIONS = {
            **Hive.Parser.FUNCTIONS,
            # "FUNCNAME": lambda args: exp.SomeNode(this=seq_get(args, 0), ...)
            "GETDATE": lambda args: exp.CurrentTimestamp(),
            "DATEADD": lambda args: exp.TsOrDsAdd(
                this=seq_get(args, 0),       # 1st arg: date
                expression=seq_get(args, 1), # 2nd arg: delta
                unit=seq_get(args, 2),       # 3rd arg: unit string
            ),
        }
        # For DDL properties like CREATE TABLE ... LIFECYCLE 30:
        PROPERTY_PARSERS = {
            **Hive.Parser.PROPERTY_PARSERS,
            "LIFECYCLE": lambda self: self.expression(
                exp.LifecycleProperty, this=self._parse_number()
            ),
        }

    class Generator(Hive.Generator):
        # Teaches the generator how to turn AST nodes back into MaxCompute SQL.
        # Reference: sqlglot/dialects/hive.py → Hive.Generator.TRANSFORMS

        # Map canonical type enum → MaxCompute type name string
        TYPE_MAPPING = {
            **Hive.Generator.TYPE_MAPPING,
            exp.DataType.Type.DATETIME: "DATETIME",
        }

        # Map expression class → SQL string (best for one-liners)
        TRANSFORMS = {
            **Hive.Generator.TRANSFORMS,
            exp.CurrentTimestamp: lambda self, e: "GETDATE()",
            exp.TsOrDsAdd: lambda self, e: self.func(
                "DATEADD", e.this, e.expression, unit_to_str(e)
            ),
        }

        # Control where DDL properties are placed in output
        PROPERTIES_LOCATION = {
            **Hive.Generator.PROPERTIES_LOCATION,
            exp.LifecycleProperty: exp.Properties.Location.POST_EXPRESSION,
        }

        # For multi-line generation logic, define a method: {classnamelower}_sql
        # Note: if both a TRANSFORM entry AND an _sql method exist, TRANSFORM wins.
        def extract_sql(self, expression: exp.Extract) -> str:
            # EXTRACT(YEAR FROM dt) → DATEPART(dt, 'year')
            unit = exp.Literal.string(expression.this.name)
            return self.func("DATEPART", expression.expression, unit)
```

---

### Key helper functions

These are imported from sqlglot internals and used throughout dialect implementations.

| Helper | Where | What it does |
|---|---|---|
| `seq_get(args, i)` | `sqlglot/helper.py` | Safe `args[i]`, returns `None` if out of bounds. Use in every Parser lambda. |
| `self.func(name, *args)` | `sqlglot/generator.py` | Generates `NAME(a, b, c)`, skipping `None` args. Use in every Generator lambda. |
| `unit_to_str(e)` | `sqlglot/dialects/dialect.py` | Extracts the unit from a date expression as a quoted string, e.g. `'day'`. |
| `rename_func(name)` | `sqlglot/dialects/dialect.py` | Shorthand TRANSFORM: renames a function keeping all args in order. |
| `build_formatted_time(exp_cls, dialect)` | `sqlglot/dialects/dialect.py` | Parser builder: constructs a time-format expression and converts the format string using `TIME_MAPPING`. |
| `build_timetostr_or_tochar` | `sqlglot/dialects/dialect.py` | Parser builder for `TO_CHAR` — picks `TimeToStr` or `ToChar` depending on the argument type. |

---

### Two ways to implement a Generator mapping

**Option A — TRANSFORMS dict** (preferred for simple cases):
```python
TRANSFORMS = {
    **Hive.Generator.TRANSFORMS,
    exp.Lower: lambda self, e: self.func("TOLOWER", e.this),
}
```

**Option B — `_sql` method** (use when logic is too complex for a one-liner):
```python
def groupconcat_sql(self, expression: exp.GroupConcat) -> str:
    # WM_CONCAT reverses the arg order: separator comes first
    sep = expression.args.get("separator") or exp.Literal.string(",")
    return self.func("WM_CONCAT", sep, expression.this)
```

> **Important**: if both exist for the same expression type, the `TRANSFORMS` entry takes precedence over the `_sql` method. (See `onboarding.md` → Generator section.)

---

### How to find the right `exp.*` node for a function

1. Parse an example in the closest dialect and `repr()` the result — fastest way.
2. Search `sqlglot/expressions.py` for the class (e.g. `class TsOrDsAdd`) and read its `arg_types`.
3. Search existing dialect files for the function name — see how other dialects map it.

```python
# Step 1: repr trick
from sqlglot import parse_one
repr(parse_one("SELECT DATE_ADD('2024-01-01', 3)", dialect="hive"))
# → look for the node class name in the output
```

---

### Reference implementations to read

| File | Why read it |
|---|---|
| `sqlglot/dialects/hive.py` | Our base — everything MaxCompute inherits or overrides |
| `sqlglot/dialects/spark.py` | A Hive subclass; shows the minimal-override pattern |
| `sqlglot/dialects/bigquery.py` | Most thorough Generator; good reference for `_sql` methods and `TYPE_MAPPING` |
| `sqlglot/dialects/dialect.py` | All shared helper builders and base `Dialect` class flags |
| `sqlglot/expressions.py` | Every `exp.*` node with its `arg_types` |
| `sqlglot/tokens.py` | `TokenType` enum — needed when adding Tokenizer keywords |

---

### Testing

```python
import sqlglot

# Transpile test: another dialect → MaxCompute
assert sqlglot.transpile(
    "SELECT CURRENT_TIMESTAMP()", read="hive", write="maxcompute"
)[0] == "SELECT GETDATE()"

# Round-trip: MaxCompute → MaxCompute (verify parser + generator are consistent)
assert sqlglot.transpile(
    "SELECT GETDATE()", read="maxcompute", write="maxcompute"
)[0] == "SELECT GETDATE()"

# Parse test: check the AST node type directly
ast = sqlglot.parse_one("SELECT DATEADD(dt, 1, 'day')", read="maxcompute")
sel = ast.selects[0]
assert isinstance(sel, sqlglot.exp.TsOrDsAdd)
```

For test file style, look at `tests/dialects/test_hive.py` in the upstream sqlglot repo.

---

## Part 2 — MaxCompute vs Hive Differences Checklist

This checklist is scoped to what MaxCompute does **differently** from Hive.
Items that Hive already handles correctly are omitted — `MaxCompute(Hive)` inherits them for free.

Legend: **[P]** = Property · **[T]** = Tokenizer · **[Pa]** = Parser · **[G]** = Generator

---

### Dialect properties

| # | Component | Item | Notes |
|---|---|---|---|
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
| 11 | `DATEDIFF(date1, date2, unit?)` | date1,date2,unit | `exp.DateDiff` | `DATEDIFF(d1,d2)` — **no unit arg** | Unit is optional 3rd arg (not 1st). Hive's entry must be overridden. |
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
