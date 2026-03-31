from __future__ import annotations

import re
import typing as t

from sqlglot import exp
from sqlglot.dialects.hive import Hive
from sqlglot.dialects.dialect import build_timetostr_or_tochar
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType


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
) -> exp.DateAdd | exp.TimestampAdd | exp.DatetimeAdd | exp.TsOrDsAdd:
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
) -> exp.DateTrunc | exp.TimestampTrunc | exp.DatetimeTrunc:
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


class MaxComputeParser(Hive.Parser):
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
        # Hive override: TO_DATE return type depends on args:
        #   TO_DATE(str)       → DATE   → TsOrDsToDate (no format)
        #   TO_DATE(str, fmt)  → DATETIME → StrToTime (format present)
        "TO_DATE": lambda args: (
            exp.StrToTime(this=seq_get(args, 0), format=seq_get(args, 1))
            if seq_get(args, 1) is not None
            else exp.TsOrDsToDate(this=seq_get(args, 0))
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
        # SUBSTR is the preferred MaxCompute alias for SUBSTRING
        "SUBSTR": exp.Substring.from_arg_list,
        # Aggregate
        "WM_CONCAT": lambda args: exp.GroupConcat(
            this=seq_get(args, 1), separator=seq_get(args, 0)
        ),
        "COUNT_IF": exp.CountIf.from_arg_list,
        "ARG_MAX": exp.ArgMax.from_arg_list,
        "ARG_MIN": exp.ArgMin.from_arg_list,
        "MAX_BY": exp.ArgMax.from_arg_list,
        "MIN_BY": exp.ArgMin.from_arg_list,
        "ANY_VALUE": exp.AnyValue.from_arg_list,
        "APPROX_DISTINCT": exp.ApproxDistinct.from_arg_list,
        "STDDEV_SAMP": exp.StddevSamp.from_arg_list,
        "COVAR_POP": exp.CovarPop.from_arg_list,
        "COVAR_SAMP": exp.CovarSamp.from_arg_list,
        "CORR": exp.Corr.from_arg_list,
        "MEDIAN": exp.Median.from_arg_list,
        "PERCENTILE_APPROX": exp.ApproxQuantile.from_arg_list,
        "BITWISE_AND_AGG": exp.BitwiseAndAgg.from_arg_list,
        "BITWISE_OR_AGG": exp.BitwiseOrAgg.from_arg_list,
        "BITWISE_XOR_AGG": exp.BitwiseXorAgg.from_arg_list,
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
        "ARRAY_INTERSECT": exp.ArrayIntersect.from_arg_list,
        "ARRAY_POSITION": exp.ArrayPosition.from_arg_list,
        "ARRAY_REMOVE": exp.ArrayRemove.from_arg_list,
        "ARRAY_CONTAINS": exp.ArrayContains.from_arg_list,
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
        return self._parse_auto_property()

    def _parse_range_clustered_by(self) -> exp.ClusteredByProperty:
        if not self._match_text_seq("CLUSTERED"):
            self._retreat(self._index - 1)
            return self._parse_dict_range(this="RANGE")
        prop = self._parse_clustered_by()
        prop.args["range"] = True
        return prop
