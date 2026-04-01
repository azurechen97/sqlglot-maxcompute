from __future__ import annotations

import unittest

from sqlglot import ErrorLevel, exp, parse_one


class Validator(unittest.TestCase):
    dialect = "maxcompute"

    def parse_one(self, sql: str) -> exp.Expression:
        return parse_one(sql, read=self.dialect)

    def validate_identity(self, sql: str, write_sql: str | None = None) -> exp.Expression:
        expr = self.parse_one(sql)
        self.assertEqual(write_sql or sql, expr.sql(dialect=self.dialect))
        return expr

    def validate_all(
        self, sql: str, read: dict | None = None, write: dict | None = None
    ) -> exp.Expression:
        # If read is provided, use the first dialect's SQL; otherwise use sql as-is
        if read:
            dialect, sql_to_read = next(iter(read.items()))
            expr = parse_one(sql_to_read, read=dialect)
        else:
            expr = self.parse_one(sql)
        for dialect, expected in (write or {}).items():
            with self.subTest(f"{sql!r} -> {dialect}"):
                self.assertEqual(
                    expr.sql(dialect, unsupported_level=ErrorLevel.IGNORE), expected
                )
        return expr


class TestMaxCompute(Validator):
    # -------------------------------------------------------------------------
    # Date arithmetic
    # -------------------------------------------------------------------------

    def test_date_arithmetic(self):
        # DATEADD
        expr = self.parse_one("DATEADD(dt, 1, 'day')")
        self.assertIsInstance(expr, exp.TsOrDsAdd)
        self.validate_all(
            "DATEADD(dt, 1, 'day')",
            write={
                "spark": "DATE_ADD(dt, 1)",
                "duckdb": "CAST(dt AS DATE) + INTERVAL 1 DAY",
                "hive": "DATE_ADD(dt, 1)",
            },
        )

        # DATEDIFF
        expr = self.parse_one("DATEDIFF(dt1, dt2)")
        self.assertIsInstance(expr, exp.DateDiff)
        self.validate_all(
            "DATEDIFF(dt1, dt2)",
            write={
                "spark": "DATEDIFF(dt1, dt2)",
                "duckdb": "DATE_DIFF('DAY', dt2, dt1)",
                "hive": "DATEDIFF(dt1, dt2)",
            },
        )

        # ADD_MONTHS
        expr = self.parse_one("ADD_MONTHS(dt, 3)")
        self.assertIsInstance(expr, exp.AddMonths)
        self.validate_all(
            "ADD_MONTHS(dt, 3)",
            write={
                "spark": "ADD_MONTHS(dt, 3)",
                "duckdb": "dt + INTERVAL 3 MONTH",
            },
        )

        # MONTHS_BETWEEN
        expr = self.parse_one("MONTHS_BETWEEN(dt1, dt2)")
        self.assertIsInstance(expr, exp.MonthsBetween)
        self.validate_all(
            "MONTHS_BETWEEN(dt1, dt2)",
            write={"spark": "MONTHS_BETWEEN(dt1, dt2)"},
        )

    # -------------------------------------------------------------------------
    # Date extraction
    # -------------------------------------------------------------------------

    def test_date_extraction(self):
        # DATEPART
        expr = self.parse_one("DATEPART(dt, 'year')")
        self.assertIsInstance(expr, exp.Extract)
        self.validate_all(
            "DATEPART(dt, 'year')",
            write={
                "spark": "EXTRACT(YEAR FROM dt)",
                "duckdb": "EXTRACT(YEAR FROM dt)",
            },
        )

        # WEEKDAY → (DAYOFWEEK(dt) + 5) % 7 in other dialects, but round-trips as WEEKDAY in MaxCompute
        expr = self.parse_one("WEEKDAY(dt)")
        self.assertIsInstance(expr, exp.Mod)
        self.validate_all(
            "WEEKDAY(dt)",
            write={
                "spark": "(DAYOFWEEK(dt) + 5) % 7",
                "duckdb": "(DAYOFWEEK(dt) + 5) % 7",
                "maxcompute": "WEEKDAY(dt)",
            },
        )

        # DATETRUNC / TRUNC_TIME
        expr = self.parse_one("DATETRUNC(dt, 'year')")
        self.assertIsInstance(expr, exp.DateTrunc)
        self.validate_all(
            "DATETRUNC(dt, 'year')",
            write={
                "spark": "TRUNC(dt, 'YEAR')",
                "duckdb": "DATE_TRUNC('YEAR', dt)",
            },
        )

        expr = self.parse_one("TRUNC_TIME(dt, 'week')")
        self.assertIsInstance(expr, exp.DateTrunc)
        self.validate_all(
            "TRUNC_TIME(dt, 'week')",
            write={
                "spark": "TRUNC(dt, WEEK(MONDAY))",
                "duckdb": "DATE_TRUNC('WEEK', dt)",
            },
        )

        # Individual extractors — parse only
        extractors = [
            ("DAYOFMONTH(dt)", exp.DayOfMonth),
            ("DAYOFWEEK(dt)", exp.DayOfWeek),
            ("DAYOFYEAR(dt)", exp.DayOfYear),
            ("HOUR(dt)", exp.Hour),
            ("MINUTE(dt)", exp.Minute),
            ("SECOND(dt)", exp.Second),
            ("QUARTER(dt)", exp.Quarter),
            ("WEEKOFYEAR(dt)", exp.WeekOfYear),
        ]
        for sql, cls in extractors:
            with self.subTest(sql):
                self.assertIsInstance(self.parse_one(sql), cls)

        self.validate_all(
            "DAYOFMONTH(dt)",
            write={"spark": "DAYOFMONTH(dt)", "duckdb": "DAYOFMONTH(dt)"},
        )
        self.validate_all(
            "WEEKOFYEAR(dt)",
            write={"spark": "WEEKOFYEAR(dt)", "duckdb": "WEEKOFYEAR(dt)"},
        )

        # LAST_DAY / LASTDAY (alias)
        self.assertIsInstance(self.parse_one("LAST_DAY(dt)"), exp.LastDay)
        self.assertIsInstance(self.parse_one("LASTDAY(dt)"), exp.LastDay)
        self.validate_all(
            "LAST_DAY(dt)",
            write={"spark": "LAST_DAY(dt)", "duckdb": "LAST_DAY(dt)"},
        )

        # NEXT_DAY
        self.assertIsInstance(self.parse_one("NEXT_DAY(dt, 'monday')"), exp.NextDay)
        self.validate_all(
            "NEXT_DAY(dt, 'monday')",
            write={"spark": "NEXT_DAY(dt, 'monday')"},
        )

    # -------------------------------------------------------------------------
    # Current date/time
    # -------------------------------------------------------------------------

    def test_current_datetime(self):
        # GETDATE and CURRENT_TIMESTAMP → same node
        for sql in ("GETDATE()", "CURRENT_TIMESTAMP()"):
            with self.subTest(sql):
                self.assertIsInstance(self.parse_one(sql), exp.CurrentTimestamp)

        self.validate_all(
            "GETDATE()",
            write={"spark": "CURRENT_TIMESTAMP()", "duckdb": "CURRENT_TIMESTAMP"},
        )

        # NOW → CurrentTimestamp (not CurrentDatetime, which is BigQuery-specific)
        self.assertIsInstance(self.parse_one("NOW()"), exp.CurrentTimestamp)
        self.validate_all(
            "NOW()",
            write={"spark": "CURRENT_TIMESTAMP()", "duckdb": "CURRENT_TIMESTAMP"},
        )

        # CURRENT_TIMEZONE
        self.assertIsInstance(self.parse_one("CURRENT_TIMEZONE()"), exp.CurrentTimezone)
        self.validate_all(
            "CURRENT_TIMEZONE()",
            write={"spark": "CURRENT_TIMEZONE()", "duckdb": "CURRENT_TIMEZONE()"},
        )

    # -------------------------------------------------------------------------
    # Bug fixes
    # -------------------------------------------------------------------------

    def test_031_fixes(self):
        # NOW() should map to CurrentTimestamp so cross-dialect output is CURRENT_TIMESTAMP,
        # not CURRENT_DATETIME() which is BigQuery-specific.
        expr = self.parse_one("NOW()")
        self.assertIsInstance(expr, exp.CurrentTimestamp)
        self.validate_all(
            "NOW()",
            write={
                "maxcompute": "GETDATE()",
                "spark": "CURRENT_TIMESTAMP()",
                "hive": "CURRENT_TIMESTAMP()",
                "duckdb": "CURRENT_TIMESTAMP",
            },
        )

    # -------------------------------------------------------------------------
    # Date/time conversion
    # -------------------------------------------------------------------------

    def test_date_conversion(self):
        # DATE_FORMAT
        self.assertIsInstance(self.parse_one("DATE_FORMAT(dt, 'yyyy-mm-dd')"), exp.TimeToStr)
        self.validate_all(
            "DATE_FORMAT(dt, 'yyyy-mm-dd')",
            write={
                "spark": "DATE_FORMAT(dt, 'yyyy-mm-dd')",
                "duckdb": "STRFTIME(dt, 'yyyy-mm-dd')",
            },
        )

        # FROM_UNIXTIME
        self.assertIsInstance(self.parse_one("FROM_UNIXTIME(1234567890)"), exp.UnixToTime)
        self.validate_all(
            "FROM_UNIXTIME(1234567890)",
            write={
                "spark": "CAST(FROM_UNIXTIME(1234567890) AS TIMESTAMP)",
                "duckdb": "TO_TIMESTAMP(1234567890)",
            },
        )

        # TO_MILLIS
        self.assertIsInstance(self.parse_one("TO_MILLIS(dt)"), exp.UnixMillis)
        self.validate_all(
            "TO_MILLIS(dt)",
            write={"spark": "UNIX_MILLIS(dt)", "duckdb": "EPOCH_MS(dt)"},
        )

        # FROM_UTC_TIMESTAMP
        expr = self.parse_one("FROM_UTC_TIMESTAMP(dt, 'Asia/Shanghai')")
        self.assertIsInstance(expr, exp.ConvertTimezone)
        self.validate_all(
            "FROM_UTC_TIMESTAMP(dt, 'Asia/Shanghai')",
            write={
                "spark": "CONVERT_TIMEZONE('UTC', 'Asia/Shanghai', dt)",
            },
        )

        # TO_DATE without format → DATE (TsOrDsToDate)
        expr = self.parse_one("TO_DATE('2024-01-01')")
        self.assertIsInstance(expr, exp.TsOrDsToDate)
        self.assertIsNone(expr.args.get("format"))
        self.validate_all(
            "TO_DATE('2024-01-01')",
            write={
                "maxcompute": "TO_DATE('2024-01-01')",
                "spark": "TO_DATE('2024-01-01')",
            },
        )

        # TO_DATE with format → DATETIME (StrToTime); format stored as MaxCompute style, not strftime
        expr = self.parse_one("TO_DATE('20240101', 'yyyymmdd')")
        self.assertIsInstance(expr, exp.StrToTime)
        self.assertEqual(expr.args.get("format").this, "yyyymmdd")
        self.validate_all(
            "TO_DATE('20240101', 'yyyymmdd')",
            write={
                "maxcompute": "TO_DATE('20240101', 'yyyymmdd')",
                "spark": "TO_TIMESTAMP('20240101', 'yyyymmdd')",
            },
        )

        # TO_CHAR (untyped arg → ToChar)
        self.assertIsInstance(self.parse_one("TO_CHAR(dt, 'yyyy-mm-dd')"), exp.ToChar)

        # ISDATE → NOT (TsOrDsToDate(...) IS NULL)
        expr = self.parse_one("ISDATE(s, 'yyyy-mm-dd')")
        self.assertIsInstance(expr, exp.Not)
        self.validate_all(
            "ISDATE(s, 'yyyy-mm-dd')",
            write={"spark": "NOT TO_DATE(s, 'yyyy-mm-dd') IS NULL"},
        )

    # -------------------------------------------------------------------------
    # String functions
    # -------------------------------------------------------------------------

    def test_string_functions(self):
        # TOLOWER / TOUPPER
        self.assertIsInstance(self.parse_one("TOLOWER(s)"), exp.Lower)
        self.assertIsInstance(self.parse_one("TOUPPER(s)"), exp.Upper)
        self.validate_all(
            "TOLOWER(s)",
            write={"spark": "LOWER(s)", "duckdb": "LOWER(s)"},
        )
        self.validate_all(
            "TOUPPER(s)",
            write={"spark": "UPPER(s)", "duckdb": "UPPER(s)"},
        )

        # REGEXP_COUNT
        self.assertIsInstance(self.parse_one("REGEXP_COUNT(s, '[0-9]+')"), exp.RegexpCount)
        self.validate_all(
            "REGEXP_COUNT(s, '[0-9]+')",
            write={"spark": "REGEXP_COUNT(s, '[0-9]+')"},
        )

        # SPLIT_PART
        self.assertIsInstance(self.parse_one("SPLIT_PART(s, ',', 1)"), exp.SplitPart)
        self.validate_all(
            "SPLIT_PART(s, ',', 1)",
            write={"spark": "SPLIT_PART(s, ',', 1)", "duckdb": "SPLIT_PART(s, ',', 1)"},
        )

    # -------------------------------------------------------------------------
    # Aggregate functions
    # -------------------------------------------------------------------------

    def test_aggregate_functions(self):
        # WM_CONCAT(sep, col) → GroupConcat
        expr = self.parse_one("WM_CONCAT(',', col)")
        self.assertIsInstance(expr, exp.GroupConcat)
        self.validate_all(
            "WM_CONCAT(',', col)",
            write={
                "spark": "LISTAGG(col, ',')",
                "duckdb": "LISTAGG(col, ',')",
            },
        )

        # COUNT_IF
        self.assertIsInstance(self.parse_one("COUNT_IF(x > 0)"), exp.CountIf)
        self.validate_all(
            "COUNT_IF(x > 0)",
            write={"spark": "COUNT_IF(x > 0)", "duckdb": "COUNT_IF(x > 0)"},
        )

        # ARG_MAX / ARG_MIN
        self.assertIsInstance(self.parse_one("ARG_MAX(x, y)"), exp.ArgMax)
        self.assertIsInstance(self.parse_one("ARG_MIN(x, y)"), exp.ArgMin)
        self.validate_all(
            "ARG_MAX(x, y)",
            write={"spark": "MAX_BY(x, y)", "duckdb": "ARG_MAX(x, y)"},
        )
        self.validate_all(
            "ARG_MIN(x, y)",
            write={"spark": "MIN_BY(x, y)", "duckdb": "ARG_MIN(x, y)"},
        )

        # ANY_VALUE
        self.assertIsInstance(self.parse_one("ANY_VALUE(x)"), exp.AnyValue)

        # APPROX_DISTINCT
        self.assertIsInstance(self.parse_one("APPROX_DISTINCT(x)"), exp.ApproxDistinct)
        self.validate_all(
            "APPROX_DISTINCT(x)",
            write={"spark": "APPROX_COUNT_DISTINCT(x)", "duckdb": "APPROX_COUNT_DISTINCT(x)"},
        )

        # Statistical aggregates
        self.assertIsInstance(self.parse_one("STDDEV_SAMP(x)"), exp.StddevSamp)
        self.assertIsInstance(self.parse_one("COVAR_POP(x, y)"), exp.CovarPop)
        self.assertIsInstance(self.parse_one("COVAR_SAMP(x, y)"), exp.CovarSamp)
        self.assertIsInstance(self.parse_one("CORR(x, y)"), exp.Corr)
        self.assertIsInstance(self.parse_one("MEDIAN(x)"), exp.Median)
        self.assertIsInstance(self.parse_one("PERCENTILE_APPROX(x, 0.5)"), exp.ApproxQuantile)
        self.assertIsInstance(self.parse_one("BITWISE_AND_AGG(x)"), exp.BitwiseAndAgg)
        self.assertIsInstance(self.parse_one("BITWISE_OR_AGG(x)"), exp.BitwiseOrAgg)
        self.assertIsInstance(self.parse_one("BITWISE_XOR_AGG(x)"), exp.BitwiseXorAgg)

    # -------------------------------------------------------------------------
    # Array functions
    # -------------------------------------------------------------------------

    def test_array_functions(self):
        # ALL_MATCH / ANY_MATCH
        self.assertIsInstance(self.parse_one("ALL_MATCH(arr, x -> x > 0)"), exp.ArrayAll)
        self.assertIsInstance(self.parse_one("ANY_MATCH(arr, x -> x > 0)"), exp.ArrayAny)
        self.validate_all(
            "ALL_MATCH(arr, x -> x > 0)",
            write={"spark": "ARRAY_ALL(arr, x -> x > 0)"},
        )

        # ARRAY_SORT
        self.assertIsInstance(self.parse_one("ARRAY_SORT(arr)"), exp.ArraySort)
        self.validate_all(
            "ARRAY_SORT(arr)",
            write={"spark": "ARRAY_SORT(arr)", "duckdb": "ARRAY_SORT(arr)"},
        )

        # ARRAY_DISTINCT
        self.assertIsInstance(self.parse_one("ARRAY_DISTINCT(arr)"), exp.ArrayDistinct)
        self.validate_all(
            "ARRAY_DISTINCT(arr)",
            write={"spark": "ARRAY_DISTINCT(arr)", "duckdb": "LIST_DISTINCT(arr)"},
        )

        # ARRAY_EXCEPT
        self.assertIsInstance(self.parse_one("ARRAY_EXCEPT(arr1, arr2)"), exp.ArrayExcept)
        self.validate_all(
            "ARRAY_EXCEPT(arr1, arr2)",
            write={"spark": "ARRAY_EXCEPT(arr1, arr2)"},
        )

        # ARRAY_JOIN
        self.assertIsInstance(self.parse_one("ARRAY_JOIN(arr, ',')"), exp.ArrayToString)
        self.validate_all(
            "ARRAY_JOIN(arr, ',')",
            write={"spark": "ARRAY_JOIN(arr, ',')", "duckdb": "ARRAY_TO_STRING(arr, ',')"},
        )

        # ARRAY_MAX / ARRAY_MIN
        self.assertIsInstance(self.parse_one("ARRAY_MAX(arr)"), exp.ArrayMax)
        self.assertIsInstance(self.parse_one("ARRAY_MIN(arr)"), exp.ArrayMin)
        self.validate_all(
            "ARRAY_MAX(arr)",
            write={"spark": "ARRAY_MAX(arr)", "duckdb": "LIST_MAX(arr)"},
        )

        # ARRAYS_OVERLAP
        self.assertIsInstance(self.parse_one("ARRAYS_OVERLAP(arr1, arr2)"), exp.ArrayOverlaps)
        self.validate_all(
            "ARRAYS_OVERLAP(arr1, arr2)",
            write={"spark": "arr1 && arr2", "duckdb": "arr1 && arr2"},
        )

        # ARRAYS_ZIP
        self.assertIsInstance(self.parse_one("ARRAYS_ZIP(arr1, arr2)"), exp.ArraysZip)
        self.validate_all(
            "ARRAYS_ZIP(arr1, arr2)",
            write={"spark": "ARRAYS_ZIP(arr1, arr2)"},
        )

        # SLICE
        self.assertIsInstance(self.parse_one("SLICE(arr, 1, 3)"), exp.ArraySlice)
        self.validate_all(
            "SLICE(arr, 1, 3)",
            write={"spark": "SLICE(arr, 1, 3)", "duckdb": "ARRAY_SLICE(arr, 1, 3)"},
        )

    # -------------------------------------------------------------------------
    # Map functions
    # -------------------------------------------------------------------------

    def test_map_functions(self):
        self.assertIsInstance(self.parse_one("MAP_CONCAT(m1, m2)"), exp.MapCat)
        self.validate_all(
            "MAP_CONCAT(m1, m2)",
            write={"spark": "MAP_CAT(m1, m2)"},
        )

        self.assertIsInstance(self.parse_one("MAP_FROM_ENTRIES(arr)"), exp.MapFromEntries)
        self.validate_all(
            "MAP_FROM_ENTRIES(arr)",
            write={"spark": "MAP_FROM_ENTRIES(arr)", "duckdb": "MAP_FROM_ENTRIES(arr)"},
        )

    # -------------------------------------------------------------------------
    # Miscellaneous functions
    # -------------------------------------------------------------------------

    def test_misc_functions(self):
        # FROM_JSON
        self.assertIsInstance(self.parse_one("FROM_JSON(s, 'schema')"), exp.ParseJSON)

        # GET_USER_ID
        self.assertIsInstance(self.parse_one("GET_USER_ID()"), exp.CurrentUser)
        self.validate_all(
            "GET_USER_ID()",
            write={"spark": "CURRENT_USER()", "duckdb": "CURRENT_USER()"},
        )

        # REGEXP_SUBSTR
        self.assertIsInstance(self.parse_one("REGEXP_SUBSTR(s, '[0-9]+')"), exp.RegexpExtract)
        self.validate_all(
            "REGEXP_SUBSTR(s, '[0-9]+')",
            write={
                "spark": "REGEXP_EXTRACT(s, '[0-9]+')",
                "duckdb": "REGEXP_EXTRACT(s, '[0-9]+')",
            },
        )

        # DAY / MONTH / YEAR (Hive wraps these in TsOrDsToDate; MaxCompute does not)
        self.assertIsInstance(self.parse_one("DAY(dt)"), exp.Day)
        self.assertIsInstance(self.parse_one("MONTH(dt)"), exp.Month)
        self.assertIsInstance(self.parse_one("YEAR(dt)"), exp.Year)


    # -------------------------------------------------------------------------
    # DDL properties
    # -------------------------------------------------------------------------

    def test_lifecycle_property(self):
        # Parse: LIFECYCLE clause produces an exp.Property with Var("LIFECYCLE") as key
        expr = parse_one("CREATE TABLE t (id INT) LIFECYCLE 30", read="maxcompute")
        props = expr.args["properties"].expressions
        lifecycle = next((p for p in props if isinstance(p, exp.Property) and p.name == "LIFECYCLE"), None)
        self.assertIsNotNone(lifecycle)
        self.assertEqual(lifecycle.args["value"].name, "30")

        # Round-trip: LIFECYCLE is preserved in MaxCompute output
        self.validate_identity("CREATE TABLE t (id INT) LIFECYCLE 30")

        # TBLPROPERTIES renders with wrapper
        self.validate_identity("CREATE TABLE t (id INT) TBLPROPERTIES ('transactional'='true')")

        # TBLPROPERTIES + LIFECYCLE together
        self.validate_identity(
            "CREATE TABLE t (id INT) TBLPROPERTIES ('transactional'='true') LIFECYCLE 7"
        )

        # Other dialects: LIFECYCLE ends up in TBLPROPERTIES (not an error)
        expr2 = parse_one("CREATE TABLE t (id INT) LIFECYCLE 30", read="maxcompute")
        hive_out = expr2.sql("hive")
        self.assertIn("LIFECYCLE", hive_out)

    def test_range_clustered_by(self):
        # Parse: produces ClusteredByProperty with range flag
        expr = parse_one(
            "CREATE TABLE t (a INT) RANGE CLUSTERED BY (a) SORTED BY (a) INTO 1024 BUCKETS",
            read="maxcompute",
        )
        prop = expr.args["properties"].expressions[0]
        self.assertIsInstance(prop, exp.ClusteredByProperty)
        self.assertTrue(prop.args.get("range"))

        # Round-trip: full syntax
        self.validate_identity(
            "CREATE TABLE t (a INT) RANGE CLUSTERED BY (a) SORTED BY (a) INTO 1024 BUCKETS"
        )

        # Round-trip: without SORTED BY
        self.validate_identity(
            "CREATE TABLE t (a INT) RANGE CLUSTERED BY (a) INTO 512 BUCKETS"
        )

        # Hash CLUSTERED BY still works (inherited from Hive)
        self.validate_identity(
            "CREATE TABLE t (a INT) CLUSTERED BY (a) SORTED BY (a) INTO 1024 BUCKETS"
        )


    def test_auto_partitioned_by(self):
        # Parse: produces PartitionedByProperty with DateTrunc child
        expr = parse_one(
            "CREATE TABLE t (a INT, dt DATETIME) AUTO PARTITIONED BY (TRUNC_TIME(dt, 'month'))",
            read="maxcompute",
        )
        prop = next(
            p
            for p in expr.args["properties"].expressions
            if isinstance(p, exp.PartitionedByProperty)
        )
        self.assertIsInstance(prop.this, (exp.DateTrunc, exp.TimestampTrunc, exp.DatetimeTrunc))

        # Round-trip: without AS
        self.validate_identity(
            "CREATE TABLE t (a INT, dt DATETIME) AUTO PARTITIONED BY (TRUNC_TIME(dt, 'month'))"
        )

        # Round-trip: with AS alias
        self.validate_identity(
            "CREATE TABLE t (a INT, dt DATETIME) AUTO PARTITIONED BY (TRUNC_TIME(dt, 'month') AS pt)"
        )

        # Regular PARTITIONED BY still works (inherited from Hive)
        self.validate_identity(
            "CREATE TABLE t (a INT) PARTITIONED BY (dt STRING)"
        )


    def test_combined_properties(self):
        # LIFECYCLE + CLUSTERED BY
        self.validate_identity(
            "CREATE TABLE t (a INT) CLUSTERED BY (a) SORTED BY (a) INTO 64 BUCKETS LIFECYCLE 30"
        )

        # TBLPROPERTIES + LIFECYCLE + PARTITIONED BY
        self.validate_identity(
            "CREATE TABLE t (a INT) PARTITIONED BY (dt STRING) "
            "TBLPROPERTIES ('transactional'='true') LIFECYCLE 7"
        )

        # RANGE CLUSTERED BY + LIFECYCLE
        self.validate_identity(
            "CREATE TABLE t (a INT) RANGE CLUSTERED BY (a) SORTED BY (a) INTO 1024 BUCKETS LIFECYCLE 30"
        )

    # -------------------------------------------------------------------------
    # Date/time round-trip tests (Generator transforms)
    # -------------------------------------------------------------------------

    def test_datediff_roundtrip(self):
        # DATEDIFF round-trips without unit (default day diff)
        self.validate_identity("SELECT DATEDIFF(dt1, dt2)")

        # DATEDIFF with explicit unit round-trips natively (not via MONTHS_BETWEEN)
        self.validate_identity("SELECT DATEDIFF(dt1, dt2, 'MONTH')")
        self.validate_identity("SELECT DATEDIFF(dt1, dt2, 'YEAR')")

    def test_dateadd_roundtrip(self):
        # DATEADD round-trips with unit preserved
        self.validate_identity("SELECT DATEADD(dt, 1, 'DAY')")
        self.validate_identity("SELECT DATEADD(dt, 3, 'MONTH')")
        self.validate_identity("SELECT DATEADD(dt, 1, 'YEAR')")

        # Cross-dialect: Spark DateAdd → MaxCompute DATEADD
        self.validate_all(
            "SELECT DATE_ADD(dt, 1)",
            read={"spark": "SELECT DATE_ADD(dt, 1)"},
            write={"maxcompute": "SELECT DATEADD(dt, 1, 'DAY')"},
        )

        # Cross-dialect: BigQuery DATE_SUB → MaxCompute DATEADD with negated delta
        self.validate_all(
            "SELECT DATE_SUB(dt, INTERVAL 3 DAY)",
            read={"bigquery": "SELECT DATE_SUB(dt, INTERVAL 3 DAY)"},
            write={"maxcompute": "SELECT DATEADD(dt, -3, 'DAY')"},
        )

    def test_datetrunc_roundtrip(self):
        # DATETRUNC round-trips with MaxCompute arg order: DATETRUNC(dt, 'UNIT')
        self.validate_identity("SELECT DATETRUNC(dt, 'YEAR')")
        self.validate_identity("SELECT DATETRUNC(dt, 'MONTH')")

        # Week unit: 'week' normalizes to 'week(monday)' on round-trip (both mean the same)
        self.validate_all(
            "SELECT DATETRUNC(dt, 'week')",
            write={"maxcompute": "SELECT DATETRUNC(dt, 'week(monday)')"},
        )
        self.validate_identity("SELECT DATETRUNC(dt, 'week(monday)')")

        # Cross-dialect: DuckDB DATE_TRUNC → MaxCompute DATETRUNC
        self.validate_all(
            "SELECT DATE_TRUNC('YEAR', dt)",
            read={"duckdb": "SELECT DATE_TRUNC('YEAR', dt)"},
            write={"maxcompute": "SELECT DATETRUNC(dt, 'YEAR')"},
        )

    def test_datepart_roundtrip(self):
        # DATEPART round-trips: DATEPART(dt, 'UNIT')
        self.validate_identity("SELECT DATEPART(dt, 'YEAR')")
        self.validate_identity("SELECT DATEPART(dt, 'MONTH')")

        # Cross-dialect: EXTRACT → MaxCompute DATEPART
        self.validate_all(
            "SELECT EXTRACT(YEAR FROM dt)",
            read={"spark": "SELECT EXTRACT(YEAR FROM dt)"},
            write={"maxcompute": "SELECT DATEPART(dt, 'YEAR')"},
        )

    def test_getdate_roundtrip(self):
        self.validate_identity("SELECT GETDATE()")

        # Cross-dialect: CURRENT_TIMESTAMP → GETDATE
        self.validate_all(
            "SELECT CURRENT_TIMESTAMP()",
            read={"spark": "SELECT CURRENT_TIMESTAMP()"},
            write={"maxcompute": "SELECT GETDATE()"},
        )

        # NOW → GETDATE (both map to CurrentTimestamp; MaxCompute generator prefers GETDATE)
        self.validate_all(
            "SELECT NOW()",
            write={"maxcompute": "SELECT GETDATE()"},
        )

    def test_string_roundtrip(self):
        self.validate_identity("SELECT TOLOWER(s)")
        self.validate_identity("SELECT TOUPPER(s)")

        # Cross-dialect: LOWER → TOLOWER
        self.validate_all(
            "SELECT LOWER(s)",
            read={"spark": "SELECT LOWER(s)"},
            write={"maxcompute": "SELECT TOLOWER(s)"},
        )

        self.validate_all(
            "SELECT UPPER(s)",
            read={"spark": "SELECT UPPER(s)"},
            write={"maxcompute": "SELECT TOUPPER(s)"},
        )

    def test_aggregate_roundtrip(self):
        # WM_CONCAT round-trip
        self.validate_identity("SELECT WM_CONCAT(',', col)")

        # ARG_MAX / ARG_MIN round-trip
        self.validate_identity("SELECT ARG_MAX(x, y)")
        self.validate_identity("SELECT ARG_MIN(x, y)")

        # APPROX_DISTINCT round-trip
        self.validate_identity("SELECT APPROX_DISTINCT(x)")

        # Cross-dialect: MAX_BY → ARG_MAX, MIN_BY → ARG_MIN (read from Spark)
        self.validate_all(
            "SELECT MAX_BY(x, y)",
            read={"spark": "SELECT MAX_BY(x, y)"},
            write={"maxcompute": "SELECT ARG_MAX(x, y)"},
        )
        self.validate_all(
            "SELECT MIN_BY(x, y)",
            read={"spark": "SELECT MIN_BY(x, y)"},
            write={"maxcompute": "SELECT ARG_MIN(x, y)"},
        )

        # Read MaxCompute MAX_BY/MIN_BY directly (aliases in MaxCompute parser)
        self.validate_all(
            "SELECT MAX_BY(x, y)",
            write={"maxcompute": "SELECT ARG_MAX(x, y)"},
        )
        self.validate_all(
            "SELECT MIN_BY(x, y)",
            write={"maxcompute": "SELECT ARG_MIN(x, y)"},
        )

    def test_misc_roundtrip(self):
        # FROM_JSON round-trip
        self.validate_identity("SELECT FROM_JSON(s, 'schema')")

        # Cross-dialect: PARSE_JSON → FROM_JSON
        self.validate_all(
            "SELECT PARSE_JSON(s)",
            read={"spark": "SELECT PARSE_JSON(s)"},
            write={"maxcompute": "SELECT FROM_JSON(s)"},
        )

        # GET_USER_ID round-trip
        self.validate_identity("SELECT GET_USER_ID()")

        # TO_MILLIS round-trip
        self.validate_identity("SELECT TO_MILLIS(dt)")

    def test_tochar_roundtrip(self):
        # TO_CHAR round-trip (untyped column → ToChar node)
        self.validate_identity("SELECT TO_CHAR(dt, 'yyyy-mm-dd')")

        # DATE_FORMAT round-trip (preserved as-is)
        self.validate_identity("SELECT DATE_FORMAT(dt, 'yyyy-mm-dd')")

    def test_type_mappings(self):
        # VARCHAR → STRING in MaxCompute
        self.validate_all(
            "CREATE TABLE t (s VARCHAR(100))",
            read={"spark": "CREATE TABLE t (s VARCHAR(100))"},
            write={"maxcompute": "CREATE TABLE t (s STRING)"},
        )

        # CHAR → STRING
        self.validate_all(
            "CREATE TABLE t (s CHAR(10))",
            read={"spark": "CREATE TABLE t (s CHAR(10))"},
            write={"maxcompute": "CREATE TABLE t (s STRING)"},
        )

        # TEXT → STRING
        self.validate_all(
            "CREATE TABLE t (s TEXT)",
            read={"spark": "CREATE TABLE t (s TEXT)"},
            write={"maxcompute": "CREATE TABLE t (s STRING)"},
        )

        # DATETIME preserved (already works, regression check)
        self.validate_identity("CREATE TABLE t (dt DATETIME)")

    def test_additional_parser_functions(self):
        # Array functions with sqlglot equivalents
        self.assertIsInstance(self.parse_one("ARRAY_INTERSECT(arr1, arr2)"), exp.ArrayIntersect)
        self.validate_all(
            "ARRAY_INTERSECT(arr1, arr2)",
            write={"spark": "ARRAY_INTERSECT(arr1, arr2)"},
        )

        self.assertIsInstance(self.parse_one("ARRAY_POSITION(arr, 1)"), exp.ArrayPosition)
        self.validate_all(
            "ARRAY_POSITION(arr, 1)",
            write={"spark": "ARRAY_POSITION(arr, 1)"},
        )

        self.assertIsInstance(self.parse_one("ARRAY_REMOVE(arr, 1)"), exp.ArrayRemove)
        self.validate_all(
            "ARRAY_REMOVE(arr, 1)",
            write={"spark": "ARRAY_REMOVE(arr, 1)"},
        )

        self.assertIsInstance(self.parse_one("ARRAY_CONTAINS(arr, 1)"), exp.ArrayContains)
        self.validate_all(
            "ARRAY_CONTAINS(arr, 1)",
            write={"spark": "ARRAY_CONTAINS(arr, 1)"},
        )

    # -------------------------------------------------------------------------
    # Comprehensive round-trip integration test
    # -------------------------------------------------------------------------

    def test_full_roundtrip(self):
        """Comprehensive round-trip: parse as maxcompute, generate as maxcompute."""
        statements = [
            "SELECT DATEADD(dt, 1, 'DAY')",
            "SELECT DATETRUNC(dt, 'MONTH')",
            "SELECT DATEPART(dt, 'YEAR')",
            "SELECT GETDATE()",
            "SELECT TOLOWER(s)",
            "SELECT TOUPPER(s)",
            "SELECT WM_CONCAT(',', col)",
            "SELECT FROM_JSON(s, 'schema')",
            "SELECT GET_USER_ID()",
            "SELECT TO_MILLIS(dt)",
            "SELECT APPROX_DISTINCT(x)",
            "SELECT ARG_MAX(x, y)",
            "SELECT ARG_MIN(x, y)",
            "SELECT DATEDIFF(dt1, dt2)",
            "SELECT DATE_FORMAT(dt, 'yyyy-mm-dd')",
            "SELECT FROM_UNIXTIME(1234567890)",
        ]
        for sql in statements:
            with self.subTest(sql):
                self.validate_identity(sql)

        # NOW() round-trips to GETDATE() (both map to CurrentTimestamp)
        self.validate_all(
            "SELECT NOW()",
            write={"maxcompute": "SELECT GETDATE()"},
        )


    def test_generator_correctness_fixes(self):
        # SPACE: MaxCompute has native SPACE(), not REPEAT(' ', n)
        self.validate_identity("SELECT SPACE(5)")
        self.validate_all(
            "SELECT SPACE(5)",
            read={"hive": "SELECT SPACE(5)"},
            write={"maxcompute": "SELECT SPACE(5)"},
        )

        # VAR_POP: MaxCompute uses VAR_POP not VARIANCE_POP
        self.validate_identity("SELECT VAR_POP(x)")
        self.validate_all(
            "SELECT VAR_POP(x)",
            read={"spark": "SELECT VAR_POP(x)"},
            write={"maxcompute": "SELECT VAR_POP(x)"},
        )

        # VAR_SAMP: MaxCompute uses VAR_SAMP not VARIANCE
        self.validate_identity("SELECT VAR_SAMP(x)")
        self.validate_all(
            "SELECT VARIANCE(x)",
            read={"spark": "SELECT VARIANCE(x)"},
            write={"maxcompute": "SELECT VAR_SAMP(x)"},
        )

        # INSTR: MaxCompute uses INSTR(str, substr) not LOCATE(substr, str)
        self.validate_identity("SELECT INSTR(s, 'sub')")
        self.validate_all(
            "SELECT LOCATE('sub', s)",
            read={"hive": "SELECT LOCATE('sub', s)"},
            write={"maxcompute": "SELECT INSTR(s, 'sub')"},
        )

        # SUBSTR: MaxCompute uses SUBSTR not SUBSTRING
        self.validate_identity("SELECT SUBSTR(s, 1, 3)")
        self.validate_all(
            "SELECT SUBSTRING(s, 1, 3)",
            read={"spark": "SELECT SUBSTRING(s, 1, 3)"},
            write={"maxcompute": "SELECT SUBSTR(s, 1, 3)"},
        )


    def test_inherited_string_functions(self):
        """Functions that work via Hive inheritance — tested here for regression coverage."""
        # Case conversion
        self.validate_identity("SELECT INITCAP(s)")
        self.validate_identity("SELECT REVERSE(s)")
        self.validate_identity("SELECT REPEAT(s, 3)")
        self.validate_identity("SELECT SPACE(5)")  # after Task 1 fix

        # Padding
        self.validate_identity("SELECT LPAD(s, 5, '0')")
        self.validate_identity("SELECT RPAD(s, 5, '0')")

        # Trimming
        self.validate_identity("SELECT LTRIM(s)")
        self.validate_identity("SELECT RTRIM(s)")

        # Regex
        self.validate_identity("SELECT REGEXP_REPLACE(s, 'a', 'b')")
        self.validate_identity("SELECT REGEXP_EXTRACT_ALL(s, '[0-9]+')")

        # Lookup
        self.validate_identity("SELECT INSTR(s, 'sub')")  # after Task 2 fix
        self.validate_identity("SELECT FIND_IN_SET('a', 'a,b,c')")
        self.validate_identity("SELECT SUBSTR(s, 1, 3)")  # after Task 3 fix
        self.validate_identity("SELECT SUBSTRING_INDEX(s, ',', 2)")

        # Misc
        self.validate_identity("SELECT CONCAT_WS(',', s1, s2)")
        self.validate_identity("SELECT FORMAT_NUMBER(1234567, 2)")

        # Cross-dialect: Spark INITCAP → MaxCompute INITCAP
        self.validate_all(
            "SELECT INITCAP(s)",
            read={"spark": "SELECT INITCAP(s)"},
            write={"maxcompute": "SELECT INITCAP(s)"},
        )


    def test_inherited_aggregate_functions(self):
        """Aggregate functions that work via Hive inheritance."""
        # Collection
        self.validate_identity("SELECT COLLECT_LIST(x)")
        self.validate_identity("SELECT COLLECT_SET(x)")

        # Variance / stddev family
        self.validate_identity("SELECT VAR_SAMP(x)")   # after Task 1 fix
        self.validate_identity("SELECT VAR_POP(x)")    # after Task 1 fix
        self.validate_identity("SELECT VARIANCE(x)", "SELECT VAR_SAMP(x)")  # VARIANCE is alias
        self.validate_identity("SELECT STDDEV(x)")

        # Percentile
        self.validate_identity("SELECT PERCENTILE(x, 0.5)")

        # Cross-dialect
        self.validate_all(
            "SELECT COLLECT_LIST(x)",
            read={"spark": "SELECT COLLECT_LIST(x)"},
            write={"maxcompute": "SELECT COLLECT_LIST(x)"},
        )
        self.validate_all(
            "SELECT COLLECT_SET(x)",
            read={"spark": "SELECT COLLECT_SET(x)"},
            write={"maxcompute": "SELECT COLLECT_SET(x)"},
        )

    def test_inherited_math_functions(self):
        """Math functions that work via Hive inheritance."""
        self.validate_identity("SELECT GREATEST(a, b)")
        self.validate_identity("SELECT LEAST(a, b)")
        self.validate_identity("SELECT CBRT(8)")
        self.validate_identity("SELECT FACTORIAL(5)")
        self.validate_all(
            "SELECT LOG2(8)",
            write={"maxcompute": "SELECT LOG(2, 8)"},  # no exp.Log2 node; LOG(base, x) is valid MC
        )

    def test_inherited_json_functions(self):
        """JSON functions that work via Hive inheritance."""
        self.validate_identity("SELECT GET_JSON_OBJECT(s, '$.key')")
        self.validate_identity("SELECT JSON_TUPLE(s, 'k1', 'k2')")


if __name__ == "__main__":
    unittest.main()
