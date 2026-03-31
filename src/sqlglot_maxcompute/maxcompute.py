from __future__ import annotations

from sqlglot.dialects.hive import Hive
from sqlglot.tokens import TokenType

from sqlglot_maxcompute.generator import MaxComputeGenerator
from sqlglot_maxcompute.parser import MaxComputeParser


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

    Parser = MaxComputeParser
    Generator = MaxComputeGenerator
