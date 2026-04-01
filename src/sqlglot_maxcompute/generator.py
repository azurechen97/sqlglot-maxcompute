from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.dialects.hive import Hive
from sqlglot.dialects.dialect import rename_func, unit_to_str
from sqlglot.transforms import (
    move_schema_columns_to_partitioned_by,
    preprocess,
    remove_unique_constraints,
    ctas_with_tmp_tables_to_create_tmp_view,
)


_AUTO_PARTITION_TYPES = (exp.DateTrunc, exp.TimestampTrunc, exp.DatetimeTrunc, exp.Alias)


def _move_schema_columns_to_partitioned_by(expression: exp.Expr) -> exp.Expr:
    """Like the Hive transform, but skip AUTO PARTITIONED BY (where this is a DateTrunc/Alias)."""
    assert isinstance(expression, exp.Create)
    prop = expression.find(exp.PartitionedByProperty)
    if prop and isinstance(prop.this, _AUTO_PARTITION_TYPES):
        return expression
    return move_schema_columns_to_partitioned_by(expression)


class MaxComputeGenerator(Hive.Generator):
    TYPE_MAPPING = {
        **Hive.Generator.TYPE_MAPPING,
        exp.DType.DATETIME: "DATETIME",
        exp.DType.VARCHAR: "STRING",
        exp.DType.CHAR: "STRING",
        exp.DType.TEXT: "STRING",
    }

    TRANSFORMS = {
        **Hive.Generator.TRANSFORMS,
        exp.Create: preprocess(
            [
                remove_unique_constraints,
                ctas_with_tmp_tables_to_create_tmp_view,
                _move_schema_columns_to_partitioned_by,
            ]
        ),
        exp.PartitionedByProperty: lambda self, e: self._partitioned_by_sql(e),
        # Date/time transforms
        exp.TsOrDsAdd: lambda self, e: self._dateadd_sql(e),
        exp.DateAdd: lambda self, e: self._dateadd_sql(e),
        exp.TimestampAdd: lambda self, e: self._dateadd_sql(e),
        exp.DatetimeAdd: lambda self, e: self._dateadd_sql(e),
        exp.DateSub: lambda self, e: self._dateadd_sql(e),
        exp.DateDiff: lambda self, e: self._datediff_sql(e),
        exp.DateTrunc: lambda self, e: self._datetrunc_sql(e),
        exp.TimestampTrunc: lambda self, e: self._datetrunc_sql(e),
        exp.DatetimeTrunc: lambda self, e: self._datetrunc_sql(e),
        exp.CurrentTimestamp: lambda self, e: "GETDATE()",
        exp.CurrentDatetime: lambda self, e: "NOW()",
        # String transforms
        exp.Lower: rename_func("TOLOWER"),
        exp.Upper: rename_func("TOUPPER"),
        # JSON / misc
        exp.ParseJSON: rename_func("FROM_JSON"),
        exp.CurrentUser: lambda self, e: "GET_USER_ID()",
        exp.UnixMillis: rename_func("TO_MILLIS"),
        # Aggregate
        exp.ApproxDistinct: rename_func("APPROX_DISTINCT"),
        exp.ArgMax: lambda self, e: self.func("ARG_MAX", e.this, e.expression),
        exp.ArgMin: lambda self, e: self.func("ARG_MIN", e.this, e.expression),
        exp.LogicalAnd: rename_func("BOOL_AND"),
        exp.LogicalOr: rename_func("BOOL_OR"),
        # Statistical aggregate fixes (Hive emits wrong names)
        exp.Space: rename_func("SPACE"),
        exp.VariancePop: rename_func("VAR_POP"),
        exp.Variance: rename_func("VAR_SAMP"),
        # Numeric truncation: TRUNC(n, d)
        exp.Trunc: lambda self, e: self.func("TRUNC", e.this, e.args.get("decimals")),
        # String position: MaxCompute uses INSTR(str, substr), not LOCATE(substr, str)
        exp.StrPosition: lambda self, e: self.func("INSTR", e.this, e.args.get("substr"), e.args.get("position")),
        # TO_DATE(str, fmt) returns DATETIME — modeled as StrToTime; emit TO_DATE in MaxCompute
        exp.StrToTime: lambda self, e: self.func("TO_DATE", e.this, e.args.get("format")),
    }

    def _dateadd_sql(
        self,
        expression: exp.TsOrDsAdd | exp.DateAdd | exp.DateSub | exp.TimestampAdd | exp.DatetimeAdd,
    ) -> str:
        unit = unit_to_str(expression) if expression.args.get("unit") else "'DAY'"
        delta = expression.expression
        if isinstance(expression, exp.DateSub):
            # DateSub magnitude is positive; negate it so DATEADD subtracts.
            # Some dialects (e.g. BigQuery) store the magnitude as a string
            # literal — normalize to a number first so we emit -3 not -'3'.
            if isinstance(delta, exp.Literal) and delta.is_string:
                delta = exp.Literal.number(delta.this)
            delta = exp.Neg(this=delta)
        return self.func("DATEADD", expression.this, delta, unit)

    def _datediff_sql(self, expression: exp.DateDiff) -> str:
        unit = unit_to_str(expression) if expression.args.get("unit") else None
        return self.func("DATEDIFF", expression.this, expression.expression, unit)

    def _datetrunc_sql(
        self, expression: exp.DateTrunc | exp.TimestampTrunc | exp.DatetimeTrunc
    ) -> str:
        unit = expression.args.get("unit")
        # WeekStart units must be emitted as 'week(day)' string literals.
        # unit_to_str returns the raw node name which would produce DATETRUNC(dt, WEEK(MONDAY))
        # — invalid MaxCompute SQL. Reconstruct the canonical 'week(day)' form instead.
        if isinstance(unit, exp.WeekStart):
            day = unit.this.name.lower() if unit.args.get("this") else "monday"
            unit_sql = exp.Literal.string(f"week({day})")
        else:
            unit_sql = unit_to_str(expression)
        return self.func("DATETRUNC", expression.this, unit_sql)

    def groupconcat_sql(self, expression: exp.GroupConcat) -> str:
        sep = expression.args.get("separator") or exp.Literal.string(",")
        return self.func("WM_CONCAT", sep, expression.this)

    def tochar_sql(self, expression: exp.ToChar) -> str:
        return self.func("TO_CHAR", expression.this, expression.args.get("format"))

    def substring_sql(self, expression: exp.Substring) -> str:
        return self.func("SUBSTR", expression.this, expression.args.get("start"), expression.args.get("length"))

    def extract_sql(self, expression: exp.Extract) -> str:
        unit = expression.this
        return self.func("DATEPART", expression.expression, exp.Literal.string(unit.name))

    def mod_sql(self, expression: exp.Mod) -> str:
        # Reverse the WEEKDAY parser transform: (DAYOFWEEK(x) + 5) % 7 → WEEKDAY(x)
        rhs = expression.expression
        lhs = expression.this
        if (
            isinstance(rhs, exp.Literal) and rhs.this == "7"
            and isinstance(lhs, exp.Paren)
            and isinstance(lhs.this, exp.Add)
            and isinstance(lhs.this.this, exp.DayOfWeek)
            and isinstance(lhs.this.expression, exp.Literal)
            and lhs.this.expression.this == "5"
        ):
            return self.func("WEEKDAY", lhs.this.this.this)
        return super().mod_sql(expression)

    def _partitioned_by_sql(self, expression: exp.PartitionedByProperty) -> str:
        inner = expression.this
        if isinstance(inner, _AUTO_PARTITION_TYPES):
            alias_sql = ""
            if isinstance(inner, exp.Alias):
                alias_sql = f" AS {inner.alias}"
                inner = inner.this
            unit = inner.args.get("unit")
            unit_str = unit.name.lower() if unit else ""
            trunc_sql = self.func("TRUNC_TIME", inner.this, exp.Literal.string(unit_str))
            return f"AUTO PARTITIONED BY ({trunc_sql}{alias_sql})"
        return f"PARTITIONED BY {self.sql(expression, 'this')}"

    def clusteredbyproperty_sql(self, expression: exp.ClusteredByProperty) -> str:
        sql = super().clusteredbyproperty_sql(expression)
        return f"RANGE {sql}" if expression.args.get("range") else sql

    def datatype_sql(self, expression: exp.DataType) -> str:
        # VARCHAR and CHAR map to STRING in MaxCompute, with no length parameters
        if expression.this in (exp.DType.VARCHAR, exp.DType.CHAR):
            return self.TYPE_MAPPING.get(expression.this, super().datatype_sql(expression))
        return super().datatype_sql(expression)

    def properties_sql(self, expression: exp.Properties) -> str:
        # Var-keyed exp.Property instances (e.g. LIFECYCLE 30) render as bare
        # KEY value after the schema. String-keyed ones stay in TBLPROPERTIES.
        var_keyed = [
            p
            for p in expression.expressions
            if isinstance(p, exp.Property) and isinstance(p.this, exp.Var)
        ]
        other = [p for p in expression.expressions if p not in var_keyed]

        other_node = exp.Properties(expressions=other)
        other_node.parent = expression.parent
        base_sql = super().properties_sql(other_node) if other else ""

        bare_sql = " ".join(f"{p.name} {self.sql(p, 'value')}" for p in var_keyed)

        if base_sql and bare_sql:
            return f"{base_sql} {bare_sql}"
        return base_sql or bare_sql
