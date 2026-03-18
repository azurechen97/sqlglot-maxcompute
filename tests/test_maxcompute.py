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

        # WEEKDAY → (DAYOFWEEK(dt) + 5) % 7
        expr = self.parse_one("WEEKDAY(dt)")
        self.assertIsInstance(expr, exp.Mod)
        self.validate_all(
            "WEEKDAY(dt)",
            write={
                "spark": "(DAYOFWEEK(dt) + 5) % 7",
                "duckdb": "(DAYOFWEEK(dt) + 5) % 7",
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

        # NOW → CurrentDatetime
        self.assertIsInstance(self.parse_one("NOW()"), exp.CurrentDatetime)
        self.validate_all(
            "NOW()",
            write={"spark": "CURRENT_DATETIME()", "duckdb": "CURRENT_DATETIME()"},
        )

        # CURRENT_TIMEZONE
        self.assertIsInstance(self.parse_one("CURRENT_TIMEZONE()"), exp.CurrentTimezone)
        self.validate_all(
            "CURRENT_TIMEZONE()",
            write={"spark": "CURRENT_TIMEZONE()", "duckdb": "CURRENT_TIMEZONE()"},
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


if __name__ == "__main__":
    unittest.main()
