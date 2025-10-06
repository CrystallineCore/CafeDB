"""
CafeDB - A lightweight, human-readable JSON database

A simple yet powerful database that stores data in JSON format with
advanced querying capabilities, thread-safety, and zero dependencies.

Example:
    >>> from cafedb import CafeDB
    >>> db = CafeDB("mydata.json")
    >>> db.create_table("users")
    >>> db.insert("users", {"name": "Alice", "age": 28})
    >>> users = db.select("users", {"age": {"$gte": 25}})
"""

from .cafedb import (
    CafeDB,
    CafeDBError,
    TableNotFoundError,
    TableExistsError,
    QueryError
)

__version__ = "0.1.0"
__author__ = "CafeDB Team"
__all__ = [
    "CafeDB",
    "CafeDBError",
    "TableNotFoundError",
    "TableExistsError",
    "QueryError",
]
