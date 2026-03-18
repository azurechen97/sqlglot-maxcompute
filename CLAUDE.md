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
- **`Generator`** — maps canonical expression nodes back to MaxCompute SQL syntax. Custom `_sql` methods handle function-level generation (e.g., `extract_sql`, `timetostr_sql`, `groupconcat_sql`).

The dialect is registered as a plugin in `pyproject.toml` under `[project.entry-points."sqlglot.dialects"]`, so after installation it is automatically discoverable by sqlglot as `"maxcompute"`.

`local/` contains development scratch files and references — **not part of the package**:
- `scratch.py` — keyword comparison scratch script
- `sqlglot/` — full clone of the sqlglot repo for reference (expressions, dialects, generator internals)

## Implementation Status

The dialect is partially implemented. Current state:
- **Parser**: 13 functions mapped (`DATEADD`, `DATEDIFF`, `DATEPART`, `DATETRUNC`, `GETDATE`, `TO_CHAR`, `TOLOWER`, `TOUPPER`, `WM_CONCAT`, `FROM_JSON`, `GET_USER_ID`, `REGEXP_SUBSTR`, `SLICE`). Note: `_build_dateadd` helper is stubbed (`pass`) and needs completing.
- **Generator**: Not yet implemented (`pass`).
- **Tests**: None yet. Test file should go in `tests/`.
- **Reference**: Full implementation checklist is in `docs/superpowers/specs/2026-03-13-maxcompute-dialect-design.md`.

## Key sqlglot patterns

When adding function mappings in `Parser.FUNCTIONS`, use `sqlglot.helper.seq_get` to safely extract positional arguments from the `args` list. Note that MaxCompute argument order sometimes differs from the canonical expression (e.g., `DATEDIFF(unit, start, end)` vs `DateDiff(this=end, expression=start, unit=unit)`).

When adding generator transforms in `Generator.TRANSFORMS`, use `self.func(name, *args)` to produce correctly formatted SQL function calls.
