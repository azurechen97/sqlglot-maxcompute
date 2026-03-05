from sqlglot.dialects.dialect import Dialect
from sqlglot.generator import Generator
from sqlglot.parser import Parser
from sqlglot.tokens import Tokenizer


class MaxCompute(Dialect):
    name = "maxcompute"

    class Tokenizer(Tokenizer):
        pass

    class Parser(Parser):
        pass

    class Generator(Generator):
        pass
