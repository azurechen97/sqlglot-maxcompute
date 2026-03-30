from __future__ import annotations

import typing as t
import re

from sqlglot import exp
from sqlglot.dialects.hive import Hive
from sqlglot.dialects.dialect import (
    build_formatted_time,
    build_timetostr_or_tochar,
    unit_to_str,
)
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType
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


WEEKDAYS = [
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
]


def _build_dateadd(
    args: t.List,
) -> exp.DateAdd | exp.TimestampAdd | exp.DatetimeAdd | exp.Anonymous:
    this = seq_get(args, 0)
    expression = seq_get(args, 1)
    unit = seq_get(args, 2)

    if this.is_type("date"):
        return exp.DateAdd(this=this, expression=expression, unit=unit)
    if this.is_type("timestamp_ntz"):
        return exp.TimestampAdd(this=this, expression=expression, unit=unit)
    if this.is_type("datetime"):
        return exp.DatetimeAdd(this=this, expression=expression, unit=unit)
    return exp.TsOrDsAdd(this=this, expression=expression, unit=unit)


def _build_datetrunc(
    args: t.List,
) -> exp.DateTrunc | exp.TimestampTrunc | exp.DatetimeTrunc | exp.Anonymous:
    this = seq_get(args, 0)
    unit = seq_get(args, 1)

    if unit.name in [f"week({weekday})" for weekday in WEEKDAYS]:
        unit = exp.WeekStart(
            this=exp.var(re.sub(r"week\((.*)\)", r"\1", unit.name).upper())
        )
    elif unit.name == "week":
        unit = exp.WeekStart(this=exp.var("MONDAY"))
    else:
        unit = exp.Var(this=unit.name.upper())

    if this.is_type("date"):
        return exp.DateTrunc(unit=unit, this=this)
    if this.is_type("timestamp_ntz"):
        return exp.TimestampTrunc(unit=unit, this=this)
    if this.is_type("datetime"):
        return exp.DatetimeTrunc(unit=unit, this=this)
    return exp.DateTrunc(unit=unit, this=this)


class MaxCompute(Hive):
    TIME_MAPPING = {
        "yyyy": "%Y",
        "yy": "%y",
        "mm": "%m",
        "dd": "%d",
        "hh": "%H",
        "mi": "%M",
        "ss": "%S",
        "ff3": "%f",
    }

    DATE_FORMAT = "'yyyy-mm-dd'"
    TIME_FORMAT = "'yyyy-mm-dd hh:mi:ss'"

    class Tokenizer(Hive.Tokenizer):
        KEYWORDS = {
            **Hive.Tokenizer.KEYWORDS,
            "EXPORT": TokenType.EXPORT,
            "LIFECYCLE": TokenType.KEY,
            "OPTION": TokenType.OPTION,
        }

    class Parser(Hive.Parser):
        FUNCTIONS = {
            **Hive.Parser.FUNCTIONS,
            # Hive overrides: MaxCompute accepts date/datetime/timestamp/string directly
            # without needing TsOrDsToDate wrapping
            "DAY": exp.Day.from_arg_list,
            "MONTH": exp.Month.from_arg_list,
            "YEAR": exp.Year.from_arg_list,
            # Hive override: MaxCompute DATE_FORMAT accepts date types directly (no TimeStrToTime)
            "DATE_FORMAT": lambda args: exp.TimeToStr(
                this=seq_get(args, 0), format=seq_get(args, 1)
            ),
            # Hive override: MaxCompute FROM_UNIXTIME takes 1 arg and returns DATETIME, not STRING
            "FROM_UNIXTIME": lambda args: exp.UnixToTime(this=seq_get(args, 0)),
            # Date arithmetic
            "DATEADD": _build_dateadd,
            "DATEDIFF": lambda args: exp.DateDiff(
                this=seq_get(args, 0),
                expression=seq_get(args, 1),
                unit=seq_get(args, 2),
                big_int=True,
            ),
            "ADD_MONTHS": exp.AddMonths.from_arg_list,
            "MONTHS_BETWEEN": exp.MonthsBetween.from_arg_list,
            # Date extraction
            "DATEPART": lambda args: exp.Extract(
                this=exp.Var(this=seq_get(args, 1).name.upper()),
                expression=seq_get(args, 0),
            ),
            "DATETRUNC": _build_datetrunc,
            "TRUNC_TIME": _build_datetrunc,
            "DAYOFMONTH": exp.DayOfMonth.from_arg_list,
            "DAYOFWEEK": exp.DayOfWeek.from_arg_list,
            "DAYOFYEAR": exp.DayOfYear.from_arg_list,
            "HOUR": exp.Hour.from_arg_list,
            "MINUTE": exp.Minute.from_arg_list,
            "SECOND": exp.Second.from_arg_list,
            "QUARTER": exp.Quarter.from_arg_list,
            "WEEKDAY": lambda args: exp.paren(exp.DayOfWeek(this=seq_get(args, 0)) + 5, copy=False) % 7,
            "WEEKOFYEAR": exp.WeekOfYear.from_arg_list,
            # Last/next day
            "LAST_DAY": exp.LastDay.from_arg_list,
            "LASTDAY": exp.LastDay.from_arg_list,
            "NEXT_DAY": exp.NextDay.from_arg_list,
            # Current date/time
            "GETDATE": lambda args: exp.CurrentTimestamp(),
            "CURRENT_TIMESTAMP": lambda args: exp.CurrentTimestamp(),
            "NOW": lambda args: exp.CurrentDatetime(),
            "CURRENT_TIMEZONE": lambda args: exp.CurrentTimezone(),
            # Conversion
            "TO_CHAR": build_timetostr_or_tochar,
            "TO_MILLIS": exp.UnixMillis.from_arg_list,
            "FROM_UTC_TIMESTAMP": lambda args: exp.ConvertTimezone(
                source_tz=exp.Literal.string("UTC"),
                target_tz=seq_get(args, 1),
                timestamp=seq_get(args, 0),
            ),
            "ISDATE": lambda args: exp.not_(
                exp.Is(
                    this=exp.TsOrDsToDate(this=seq_get(args, 0), format=seq_get(args, 1), safe=True),
                    expression=exp.Null(),
                )
            ),
            # String functions
            "TOLOWER": exp.Lower.from_arg_list,
            "TOUPPER": exp.Upper.from_arg_list,
            "REGEXP_COUNT": exp.RegexpCount.from_arg_list,
            "SPLIT_PART": exp.SplitPart.from_arg_list,
            # Aggregate
            "WM_CONCAT": lambda args: exp.GroupConcat(
                this=seq_get(args, 1), separator=seq_get(args, 0)
            ),
            "COUNT_IF": exp.CountIf.from_arg_list,
            "ARG_MAX": exp.ArgMax.from_arg_list,
            "ARG_MIN": exp.ArgMin.from_arg_list,
            "ANY_VALUE": exp.AnyValue.from_arg_list,
            "APPROX_DISTINCT": exp.ApproxDistinct.from_arg_list,
            # Array functions
            "ALL_MATCH": exp.ArrayAll.from_arg_list,
            "ANY_MATCH": exp.ArrayAny.from_arg_list,
            "ARRAY_SORT": exp.ArraySort.from_arg_list,
            "ARRAY_DISTINCT": exp.ArrayDistinct.from_arg_list,
            "ARRAY_EXCEPT": exp.ArrayExcept.from_arg_list,
            "ARRAY_JOIN": exp.ArrayToString.from_arg_list,
            "ARRAY_MAX": exp.ArrayMax.from_arg_list,
            "ARRAY_MIN": exp.ArrayMin.from_arg_list,
            "ARRAYS_OVERLAP": exp.ArrayOverlaps.from_arg_list,
            "ARRAYS_ZIP": lambda args: exp.ArraysZip(expressions=args),
            # Map functions
            "MAP_CONCAT": exp.MapCat.from_arg_list,
            "MAP_FROM_ENTRIES": exp.MapFromEntries.from_arg_list,
            # JSON / misc
            "FROM_JSON": exp.ParseJSON.from_arg_list,
            "GET_USER_ID": lambda args: exp.CurrentUser(),
            "REGEXP_SUBSTR": exp.RegexpExtract.from_arg_list,
            "SLICE": exp.ArraySlice.from_arg_list,
        }

        PROPERTY_PARSERS = {
            **Hive.Parser.PROPERTY_PARSERS,
            # LIFECYCLE n — MaxCompute table retention in days. Stored as a generic
            # exp.Property with a Var key so no custom expression class is needed and
            # sqlglot's PROPERTIES_LOCATION contract is not broken.
            "LIFECYCLE": lambda self: self.expression(
                exp.Property(this=exp.var("LIFECYCLE"), value=self._parse_number())
            ),
            "RANGE": lambda self: self._parse_range_clustered_by(),
            "AUTO": lambda self: self._parse_auto_partition(),
        }

        def _parse_auto_partition(self) -> exp.PartitionedByProperty | exp.AutoRefreshProperty | None:
            if self._match(TokenType.PARTITION_BY):
                self._match(TokenType.L_PAREN)
                expr = self._parse_conjunction()
                if self._match(TokenType.ALIAS):
                    expr = exp.Alias(this=expr, alias=self._parse_id_var())
                self._match(TokenType.R_PAREN)
                return exp.PartitionedByProperty(this=expr)
            # Fall through to base AUTO REFRESH handling
            return self._parse_auto_property()

        def _parse_range_clustered_by(self) -> exp.ClusteredByProperty:
            if not self._match_text_seq("CLUSTERED"):
                self._retreat(self._index - 1)
                return self._parse_dict_range(this="RANGE")
            prop = self._parse_clustered_by()
            prop.args["range"] = True
            return prop

    class Generator(Hive.Generator):
        TYPE_MAPPING = {
            **Hive.Generator.TYPE_MAPPING,
            exp.DType.DATETIME: "DATETIME",
        }

        PROPERTIES_LOCATION = {
            **Hive.Generator.PROPERTIES_LOCATION,
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
            exp.DateTrunc: lambda self, e: self._datetrunc_sql(e),
            exp.TimestampTrunc: lambda self, e: self._datetrunc_sql(e),
            exp.DatetimeTrunc: lambda self, e: self._datetrunc_sql(e),
            exp.CurrentTimestamp: lambda self, e: "GETDATE()",
            exp.CurrentDatetime: lambda self, e: "NOW()",
        }

        def _dateadd_sql(self, expression: exp.TsOrDsAdd | exp.DateAdd | exp.TimestampAdd | exp.DatetimeAdd) -> str:
            unit = unit_to_str(expression) if expression.args.get("unit") else "'DAY'"
            return self.func("DATEADD", expression.this, expression.expression, unit)

        def _datetrunc_sql(self, expression: exp.DateTrunc | exp.TimestampTrunc | exp.DatetimeTrunc) -> str:
            return self.func("DATETRUNC", expression.this, unit_to_str(expression))

        def extract_sql(self, expression: exp.Extract) -> str:
            unit = expression.this
            return self.func("DATEPART", expression.expression, exp.Literal.string(unit.name))

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
