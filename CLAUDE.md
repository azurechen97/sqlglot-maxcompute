# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`sqlglot-maxcompute` is a SQLGlot dialect plugin for Alibaba Cloud MaxCompute (formerly ODPS). It registers the `MaxCompute` dialect via Python entry points so that `sqlglot` can parse and generate MaxCompute SQL.

## Commands

This project uses `uv` for dependency management.

```bash
# Install dependencies (including dev)
uv sync

# Run all tests
uv run pytest

# Run a single test file
uv run pytest tests/test_foo.py

# Run a single test by name
uv run pytest tests/test_foo.py::test_bar
```

## Architecture

The dialect is split across three files in `src/sqlglot_maxcompute/`:

- **`parser.py`** — `MaxComputeParser(HiveParser)`: `FUNCTIONS` dict mapping MaxCompute function names to canonical `sqlglot.exp` nodes; `PROPERTY_PARSERS` for `LIFECYCLE`, `RANGE`, and `AUTO`; helper builders `_build_dateadd`, `_build_datetrunc`.
- **`generator.py`** — `MaxComputeGenerator(HiveGenerator)`: `TYPE_MAPPING`, `TRANSFORMS`, and named `_sql` methods that map canonical AST nodes back to MaxCompute SQL.
- **`dialect.py`** — `MaxCompute(Hive)`: slim coordinator that sets `TIME_MAPPING`/`DATE_FORMAT`/`TIME_FORMAT`, adds `Tokenizer` keywords (`EXPORT`, `LIFECYCLE`, `OPTION`), and wires `Parser = MaxComputeParser` / `Generator = MaxComputeGenerator`.

The dialect is registered as a plugin in `pyproject.toml` under `[project.entry-points."sqlglot.dialects"]`, so after installation it is automatically discoverable by sqlglot as `"maxcompute"`.

This split mirrors sqlglot's own mypyc-compile refactor (parsers/generators split into `sqlglot.parsers.*` / `sqlglot.generators.*` modules) and requires sqlglot ≥ 30.1.0.

`local/` contains development scratch files and references — **not part of the package**:
- `scratch.py` — keyword comparison scratch script
- `sqlglot/` — full clone of the sqlglot repo for reference (expressions, dialects, generator internals); `sqlglot/posts/` contains official guides (`onboarding.md` for architecture deep-dive, `ast_primer.md` for AST tutorial). Parsers live in `parsers/`, generators in `generators/`, expressions in `expressions/` package
- `ydb-sqlglot-plugin/` — YDB dialect plugin, used as reference for how a well-behaved plugin is structured
- `maxcompute_doc/` — MaxCompute official function documentation (e.g., `date_func.md`, `func_comparison.md`)

## Implementation Status

The dialect is complete at v0.4.0:
- **Parser**: ~65 functions explicitly mapped (date/time, string, aggregate, array, map); remainder inherited from Hive.
- **Generator**: `TRANSFORMS` + named `_sql` methods for all major expression types; Hive handles the rest.
- **Tests**: 40 test methods, 186 subtests covering parse, round-trip, and cross-dialect transpilation.

## Key sqlglot patterns

When adding function mappings in `Parser.FUNCTIONS`, use `sqlglot.helper.seq_get` to safely extract positional arguments from the `args` list. Note that MaxCompute argument order sometimes differs from the canonical expression (e.g., `DATEDIFF(unit, start, end)` vs `DateDiff(this=end, expression=start, unit=unit)`).

When adding generator transforms in `Generator.TRANSFORMS`, use `self.func(name, *args)` to produce correctly formatted SQL function calls.

## Testing patterns

Tests use a `Validator` base class (inline in `tests/test_maxcompute.py`) mirroring sqlglot's pattern:
- `validate_all(sql, write={dialect: expected})` — cross-dialect transpilation assertions
- `assertIsInstance(parse_one(sql, read="maxcompute"), exp.SomeClass)` — parse node assertions
- **`read=` must be a dict** — `read={"spark": "LOCATE(...)"}`, not `read="spark"`. Bare string is silently ignored by `validate_all`.
- **Pyright false positive** — `assertIsNotNone(x)` does not narrow types in Pyright; `x.field` after it shows "attribute of None" errors that are noise, not real bugs.

**Development is test-driven (TDD).** For every fix or feature:
1. Write the failing test first and run it to confirm it fails
2. Implement the minimal change to make it pass
3. Run the full suite to confirm no regressions
4. Commit

Before writing `validate_all` assertions, probe actual output first:
```bash
uv run python -c "from sqlglot import parse_one; e = parse_one('FUNC(...)', read='maxcompute'); print(e.sql('spark'))"
```

## Debugging with probe scripts

For multi-step debugging (AST inspection, tracing transforms, etc.), write a temporary script to `local/probe.py` and run it with `uv run python local/probe.py`. The `local/` directory is gitignored, so probe scripts won't pollute the repo. **Always delete when done** — subagents consistently forget to clean up.

When instructing subagents to debug, explicitly include: "write probe scripts to `local/probe.py`, run with `uv run python local/probe.py`, delete when done."

## Plugin contract — do not break sqlglot internals

This is a **dialect plugin**, not a fork. We must stay within sqlglot's public extension points:

- **No custom `exp.Property` subclasses** — all `Property` subclasses must live in sqlglot's `expressions/properties.py` and be registered in the base `Generator.PROPERTIES_LOCATION`. Defining a custom subclass in this plugin breaks every other dialect's `locate_properties` (which uses a raw dict lookup with no fallback). Use generic `exp.Property(this=exp.var("KEY"), value=...)` instead and override `TRANSFORMS[exp.Property]` and `PROPERTIES_LOCATION[exp.Property]` in `MaxCompute.Generator` to handle the formatting.
- **No monkey-patching sqlglot internals** — do not patch `Generator.locate_properties`, `Generator.TRANSFORMS`, or any other base class method/dict outside the `MaxCompute` class hierarchy.
- **No new `exp.*` expression classes** — all AST node types must be existing sqlglot classes. Check `expressions.py` before considering anything custom.

## Scraping MaxCompute docs

Alibaba help pages have a `复制为 MD 格式` button that copies the page as markdown to clipboard.
Workflow: `browser_navigate` → `browser_snapshot` (save to file, grep for button ref) → `browser_click` → `browser_evaluate(() => navigator.clipboard.readText())` → `Write` to `local/maxcompute_doc/`.
Note: snapshots exceed token limits; grep the saved file for the button ref instead of reading it directly.

## Parser authoring rules

- **Never use `exp.Anonymous`** — check `expressions.py` for a proper class first; use formula-based expressions as fallback.
- **Inherit, don't re-implement** — omit functions from `Parser.FUNCTIONS` if MaxCompute and Hive have identical semantics.
- **Type-dispatch builders** — `_build_dateadd` / `_build_datetrunc` dispatch to typed nodes via `is_type()`, with an untyped fallback.

## Generator authoring rules

- **`self.func` drops `None` args silently** — guard optional args before passing to avoid emitting invalid SQL (e.g. `groupconcat_sql` defaults `separator` to `','`).
- **`unit_to_str` on `WeekStart` returns the raw name, not a string literal** — reconstruct as `exp.Literal.string(f"week({day})")` manually.
- **Named `_sql` methods vs TRANSFORMS** — use a named method when the base class already defines one (e.g. `extract_sql`, `groupconcat_sql`); both work but the method is cleaner and avoids surprise overrides.
- **Don't add empty `PROPERTIES_LOCATION = {**Hive.Generator.PROPERTIES_LOCATION}`** — pure boilerplate; only add the dict when you have new entries to include.
- **DateSub string-literal delta (BigQuery quirk)** — BigQuery's `DATE_SUB` stores the magnitude as a string literal; normalize before negating: `exp.Literal.number(delta.this)` so you emit `-3` not `-'3'`.

## DDL design decisions

- **LIFECYCLE vs TBLPROPERTIES coexistence** — stored as `exp.Property(this=exp.var("LIFECYCLE"), value=...)`. The `properties_sql` override in `MaxComputeGenerator` separates Var-keyed properties (rendered bare as `LIFECYCLE 30`) from string-keyed ones (delegated to Hive's TBLPROPERTIES wrapper). This avoids overriding `PROPERTIES_LOCATION[exp.Property]`, which would break other dialects.
- **RANGE CLUSTERED BY** — reuses `exp.ClusteredByProperty` with an undeclared `args["range"] = True` flag. Undeclared args survive `copy()`/`deepcopy()` in sqlglot's `Expression` base. The generator's `clusteredbyproperty_sql` override prepends `RANGE ` when the flag is present.
- **AUTO PARTITIONED BY** — parsed as `PartitionedByProperty(this=DateTrunc(...))` or `PartitionedByProperty(this=Alias(this=DateTrunc(...), alias=...))`. The generator detects `DateTrunc`/`TimestampTrunc`/`DatetimeTrunc` (or `Alias` wrapping one) as the `this` child to identify auto-partition nodes and emit `AUTO PARTITIONED BY (TRUNC_TIME(...))`.
- **TO_DATE return type** — `TO_DATE(str)` → `exp.TsOrDsToDate` (DATE); `TO_DATE(str, fmt)` → `exp.StrToTime` (DATETIME). The generator maps `exp.StrToTime` back to `TO_DATE(str, fmt)` so MaxCompute output is correct and cross-dialect consumers see the right type.
