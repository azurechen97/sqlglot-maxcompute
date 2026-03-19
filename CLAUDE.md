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

The entire dialect lives in `src/sqlglot_maxcompute/maxcompute.py`. The `MaxCompute` class subclasses `sqlglot.dialects.hive.Hive` and overrides three inner classes:

- **`Tokenizer`** — adds MaxCompute-specific keywords (e.g., `EXPORT`, `OPTION`) on top of Hive's keywords.
- **`Parser`** — maps MaxCompute built-in function names to canonical `sqlglot.exp` expression nodes (e.g., `DATEADD` → `TsOrDsAdd`, `DATEDIFF` → `DateDiff`, `WM_CONCAT` → `GroupConcat`).
- **`Generator`** — will map canonical expression nodes back to MaxCompute SQL syntax via auto-discovered `<name>_sql()` methods or `TRANSFORMS` entries. Currently `pass`.

The dialect is registered as a plugin in `pyproject.toml` under `[project.entry-points."sqlglot.dialects"]`, so after installation it is automatically discoverable by sqlglot as `"maxcompute"`.

`local/` contains development scratch files and references — **not part of the package**:
- `scratch.py` — keyword comparison scratch script
- `sqlglot/` — full clone of the sqlglot repo for reference (expressions, dialects, generator internals); `sqlglot/posts/` contains official guides (`onboarding.md` for architecture deep-dive, `ast_primer.md` for AST tutorial). Note: local clone is newer than installed (30.0.1) — dialect parsers moved to `parsers/`, expressions split into `expressions/` package
- `ydb-sqlglot-plugin/` — YDB dialect plugin, used as reference for how a well-behaved plugin is structured
- `maxcompute_doc/` — MaxCompute official function documentation (e.g., `date_func.md`, `func_comparison.md`)

## Implementation Status

The dialect is partially implemented. Current state:
- **Parser**: ~50 functions mapped across date/time, string, aggregate, array, and map categories.
- **Generator**: Partially implemented — `TRANSFORMS`, `PROPERTIES_LOCATION`, and `property_sql` override for LIFECYCLE; inherits Hive's generator for everything else.
- **Tests**: `tests/test_maxcompute.py` covers all Parser mappings (parse assertions + cross-dialect transpilation).
- **Reference**: Full implementation checklist is in `docs/superpowers/specs/2026-03-13-maxcompute-dialect-design.md`.

## Key sqlglot patterns

When adding function mappings in `Parser.FUNCTIONS`, use `sqlglot.helper.seq_get` to safely extract positional arguments from the `args` list. Note that MaxCompute argument order sometimes differs from the canonical expression (e.g., `DATEDIFF(unit, start, end)` vs `DateDiff(this=end, expression=start, unit=unit)`).

When adding generator transforms in `Generator.TRANSFORMS`, use `self.func(name, *args)` to produce correctly formatted SQL function calls.

## Testing patterns

Tests use a `Validator` base class (inline in `tests/test_maxcompute.py`) mirroring sqlglot's pattern:
- `validate_all(sql, write={dialect: expected})` — cross-dialect transpilation assertions
- `assertIsInstance(parse_one(sql, read="maxcompute"), exp.SomeClass)` — parse node assertions

Before writing `validate_all` assertions, probe actual output first:
```bash
uv run python -c "from sqlglot import parse_one; e = parse_one('FUNC(...)', read='maxcompute'); print(e.sql('spark'))"
```

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
