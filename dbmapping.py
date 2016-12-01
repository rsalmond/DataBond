"""
sqlalchemy type conversion helpers
"""

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.dialects.mysql import TINYINT, ENUM

@compiles(ENUM, 'sqlite')
def compile_ENUM_mysql_sqlite(element, compiler, **kw):
    maxlen = 0
    for enum in element.enums:
        if len(enum) > maxlen:
            maxlen = len(enum)

    return compiler.visit_VARCHAR(element, length=maxlen, **kw)

@compiles(TINYINT, 'sqlite')
def compile_TINYINT_mysql_sqlite(element, compiler, **kw):
    return compiler.visit_INTEGER(element, **kw)

