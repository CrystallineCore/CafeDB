import json
import re
import threading
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Callable, Union, Optional
from contextlib import contextmanager


class CafeDBError(Exception):
    """Base exception for CafeDB errors"""
    pass


class TableNotFoundError(CafeDBError):
    """Raised when table doesn't exist"""
    pass


class TableExistsError(CafeDBError):
    """Raised when table already exists"""
    pass


class QueryError(CafeDBError):
    """Raised when query is malformed"""
    pass


class CafeDB:
    """
    CafeDB - A lightweight, human-readable JSON database
    
    Features:
    - Zero configuration required
    - Human-readable JSON storage
    - Advanced querying with operators
    - Thread-safe operations
    - Automatic backups
    - Crash-safe atomic writes
    """
    
    def __init__(self, db_path: str, verbose: bool = False, backup: bool = True):
        """
        Initialize CafeDB
        
        Args:
            db_path: Path to JSON database file
            verbose: Enable detailed logging
            backup: Create automatic backups during writes
        """
        self.db_path = Path(db_path)
        self.verbose = verbose
        self.backup = backup
        self._lock = threading.RLock()
        
        if self.db_path.exists():
            self._data = self._read_db()
        else:
            self._data = {
                "_meta": {
                    "tables": [],
                    "created": datetime.now().isoformat(),
                    "version": "1.0.0"
                }
            }
            self._write_db()
        
        if "_meta" not in self._data:
            self._data["_meta"] = {
                "tables": [],
                "created": datetime.now().isoformat(),
                "version": "1.0.0"
            }

    def _read_db(self) -> dict:
        """Read database from file with error handling"""
        try:
            with open(self.db_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise CafeDBError(f"Corrupted database file: {e}")
        except IOError as e:
            raise CafeDBError(f"Cannot read database file: {e}")

    def _write_db(self):
        """Atomic write with optional backup"""
        with self._lock:
            try:
                # Create backup if enabled and file exists
                if self.backup and self.db_path.exists():
                    backup_path = self.db_path.with_suffix(".backup")
                    self.db_path.replace(backup_path)
                
                # Write to temp file first (atomic operation)
                tmp_path = self.db_path.with_suffix(self.db_path.suffix + ".tmp")
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(self._data, f, indent=2, ensure_ascii=False)
                
                # Atomic replace
                tmp_path.replace(self.db_path)
                
                # Update metadata
                self._data["_meta"]["last_modified"] = datetime.now().isoformat()
                
                if self.verbose:
                    print(f"DB written to {self.db_path}")
                    
            except IOError as e:
                raise CafeDBError(f"Failed to write database: {e}")

    def _match_wildcard(self, value: Any, pattern: str) -> bool:
        """Match value against wildcard pattern (* and ?)"""
        if not isinstance(value, str):
            value = str(value)
        
        regex_pattern = pattern.replace('*', '.*').replace('?', '.')
        regex_pattern = f"^{regex_pattern}$"
        
        return bool(re.match(regex_pattern, value, re.IGNORECASE))
    
    def _match_condition(self, value: Any, condition: Union[Any, Dict[str, Any]]) -> bool:
        """Match value against condition with operators"""
        if isinstance(condition, dict):
            for op, op_value in condition.items():
                if op == '$eq':
                    if value != op_value:
                        return False
                elif op == '$ne':
                    if value == op_value:
                        return False
                elif op == '$gt':
                    try:
                        if not (value > op_value):
                            return False
                    except TypeError:
                        return False
                elif op == '$gte':
                    try:
                        if not (value >= op_value):
                            return False
                    except TypeError:
                        return False
                elif op == '$lt':
                    try:
                        if not (value < op_value):
                            return False
                    except TypeError:
                        return False
                elif op == '$lte':
                    try:
                        if not (value <= op_value):
                            return False
                    except TypeError:
                        return False
                elif op == '$in':
                    if value not in op_value:
                        return False
                elif op == '$nin':
                    if value in op_value:
                        return False
                elif op == '$like':
                    if not self._match_wildcard(value, op_value):
                        return False
                elif op == '$regex':
                    try:
                        if not re.search(op_value, str(value), re.IGNORECASE):
                            return False
                    except re.error as e:
                        raise QueryError(f"Invalid regex pattern '{op_value}': {e}")
                elif op == '$contains':
                    if not isinstance(value, str) or op_value.lower() not in value.lower():
                        return False
                elif op == '$startswith':
                    if not isinstance(value, str) or not value.lower().startswith(op_value.lower()):
                        return False
                elif op == '$endswith':
                    if not isinstance(value, str) or not value.lower().endswith(op_value.lower()):
                        return False
                elif op == '$between':
                    if not isinstance(op_value, (list, tuple)) or len(op_value) != 2:
                        raise QueryError("$between requires array of exactly 2 values")
                    min_val, max_val = op_value
                    try:
                        if not (min_val <= value <= max_val):
                            return False
                    except TypeError:
                        return False
                elif op == '$exists':
                    return op_value
                else:
                    raise QueryError(
                        f"Unknown operator: {op}. "
                        f"Valid operators: $eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, "
                        f"$like, $regex, $contains, $startswith, $endswith, $between, $exists"
                    )
            return True
        else:
            # Direct match or wildcard
            if isinstance(condition, str) and ('*' in condition or '?' in condition):
                return self._match_wildcard(value, condition)
            else:
                return value == condition
    
    def _build_condition_function(self, filters: Dict[str, Any]) -> Callable:
        """Build a condition function from filter dictionary"""
        def condition_func(row: dict) -> bool:
            for field, condition in filters.items():
                # Handle $exists operator specially
                if isinstance(condition, dict) and '$exists' in condition:
                    field_exists = field in row
                    if condition['$exists'] != field_exists:
                        return False
                    continue
                
                if field not in row:
                    return False
                
                if not self._match_condition(row[field], condition):
                    return False
            
            return True
        
        return condition_func

    @contextmanager
    def transaction(self):
        """
        Context manager for transaction-like operations
        
        Usage:
            with db.transaction():
                db.insert("users", user1)
                db.update("users", {...}, {...})
                # If any operation fails, all changes are rolled back
        """
        with self._lock:
            backup_data = json.loads(json.dumps(self._data))
            try:
                yield self
                self._write_db()
            except Exception as e:
                self._data = backup_data
                if self.verbose:
                    print(f"Transaction rolled back: {e}")
                raise

    def create_table(self, table_name: str):
        """
        Create a new table
        
        Args:
            table_name: Name of the table to create
            
        Raises:
            TableExistsError: If table already exists
            QueryError: If table name is invalid
        """
        with self._lock:
            if table_name in self._data:
                raise TableExistsError(
                    f"Table '{table_name}' already exists. "
                    f"Use drop_table() first or choose a different name."
                )
            
            if table_name.startswith('_'):
                raise QueryError("Table names cannot start with underscore (reserved for internal use)")
            
            self._data[table_name] = []
            self._data["_meta"]["tables"].append(table_name)
            self._data["_meta"]["tables"] = sorted(self._data["_meta"]["tables"])
            self._write_db()
            
            if self.verbose:
                print(f"Table '{table_name}' created")

    def drop_table(self, table_name: str):
        """
        Delete a table and all its data
        
        Args:
            table_name: Name of the table to drop
            
        Raises:
            TableNotFoundError: If table doesn't exist
        """
        with self._lock:
            if table_name not in self._data:
                available = ', '.join(self.list_tables()) or 'none'
                raise TableNotFoundError(
                    f"Table '{table_name}' does not exist. "
                    f"Available tables: {available}"
                )
            
            row_count = len(self._data[table_name])
            del self._data[table_name]
            self._data["_meta"]["tables"].remove(table_name)
            self._write_db()
            
            if self.verbose:
                print(f"Table '{table_name}' deleted ({row_count} rows removed)")

    def insert(self, table_name: str, row: dict):
        """
        Insert a single row into a table
        
        Args:
            table_name: Name of the table
            row: Dictionary containing the data to insert
            
        Raises:
            TableNotFoundError: If table doesn't exist
            QueryError: If row is not a dictionary
        """
        with self._lock:
            if table_name not in self._data:
                raise TableNotFoundError(
                    f"Table '{table_name}' does not exist. "
                    f"Use create_table('{table_name}') first."
                )
            
            if not isinstance(row, dict):
                raise QueryError(f"Row must be a dictionary, got {type(row).__name__}")
            
            # Add metadata
            row_with_meta = {
                **row,
                "_inserted_at": datetime.now().isoformat()
            }
            
            self._data[table_name].append(row_with_meta)
            self._write_db()
            
            if self.verbose:
                print(f"Inserted into '{table_name}': {row}")

    def insert_many(self, table_name: str, rows: List[dict]) -> int:
        """
        Batch insert multiple rows (more efficient than multiple inserts)
        
        Args:
            table_name: Name of the table
            rows: List of dictionaries to insert
            
        Returns:
            Number of rows inserted
            
        Raises:
            TableNotFoundError: If table doesn't exist
            QueryError: If rows is not a list
        """
        with self._lock:
            if table_name not in self._data:
                raise TableNotFoundError(
                    f"Table '{table_name}' does not exist. "
                    f"Use create_table('{table_name}') first."
                )
            
            if not isinstance(rows, list):
                raise QueryError(f"Rows must be a list, got {type(rows).__name__}")
            
            timestamp = datetime.now().isoformat()
            rows_with_meta = [
                {**row, "_inserted_at": timestamp}
                for row in rows
            ]
            
            self._data[table_name].extend(rows_with_meta)
            self._write_db()
            
            if self.verbose:
                print(f"Inserted {len(rows)} rows into '{table_name}'")
            
            return len(rows)

    def select(
        self, 
        table_name: str, 
        filters: Union[Dict[str, Any], Callable, None] = None,
        fields: Optional[List[str]] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        order_by: Optional[str] = None,
        reverse: bool = False
    ) -> List[dict]:
        """
        Select rows from a table with filtering, projection, pagination, and sorting
        
        Args:
            table_name: Name of the table
            filters: Filter conditions (dict) or custom function (callable)
            fields: List of field names to return (projection)
            limit: Maximum number of rows to return
            offset: Number of rows to skip
            order_by: Field name to sort by
            reverse: Sort in descending order (default: ascending)
            
        Returns:
            List of matching rows
            
        Raises:
            TableNotFoundError: If table doesn't exist
            QueryError: If filters or fields are invalid
        """
        with self._lock:
            if table_name not in self._data:
                available = ', '.join(self.list_tables()) or 'none'
                raise TableNotFoundError(
                    f"Table '{table_name}' does not exist. "
                    f"Available tables: {available}"
                )
            
            rows = self._data[table_name]
            
            # Apply filters
            if filters is not None:
                if callable(filters):
                    rows = [r for r in rows if filters(r)]
                elif isinstance(filters, dict):
                    condition_func = self._build_condition_function(filters)
                    rows = [r for r in rows if condition_func(r)]
                else:
                    raise QueryError("Filters must be dict, callable, or None")
            
            # Apply sorting
            if order_by:
                if rows and order_by not in (rows[0].keys() if rows else []):
                    if self.verbose:
                        print(f"Warning: order_by field '{order_by}' not found in rows")
                try:
                    rows = sorted(rows, key=lambda x: x.get(order_by, ''), reverse=reverse)
                except TypeError:
                    rows = sorted(rows, key=lambda x: str(x.get(order_by, '')), reverse=reverse)
            
            # Apply pagination
            if offset > 0:
                rows = rows[offset:]
            if limit is not None:
                rows = rows[:limit]
            
            # Apply field projection
            if fields is not None:
                if not isinstance(fields, list):
                    raise QueryError(f"Fields must be a list, got {type(fields).__name__}")
                rows = [{k: row.get(k) for k in fields} for row in rows]
            else:
                rows = [row.copy() for row in rows]
            
            return rows

    def update(
        self, 
        table_name: str, 
        filters: Union[Dict[str, Any], Callable], 
        updater: Union[Dict[str, Any], Callable]
    ) -> int:
        """
        Update rows matching filters
        
        Args:
            table_name: Name of the table
            filters: Filter conditions (dict) or custom function (callable)
            updater: Updates to apply (dict) or custom function (callable)
            
        Returns:
            Number of rows updated
            
        Raises:
            TableNotFoundError: If table doesn't exist
            QueryError: If filters or updater are invalid
        """
        with self._lock:
            if table_name not in self._data:
                raise TableNotFoundError(f"Table '{table_name}' does not exist")
            
            updated_count = 0
            
            # Build condition function
            if callable(filters):
                condition_func = filters
            elif isinstance(filters, dict):
                condition_func = self._build_condition_function(filters)
            else:
                raise QueryError("Filters must be dict or callable")
            
            # Build update function
            if callable(updater):
                update_func = updater
            elif isinstance(updater, dict):
                def update_func(row):
                    updated = {**row, **updater}
                    updated["_updated_at"] = datetime.now().isoformat()
                    return updated
            else:
                raise QueryError("Updater must be dict or callable")
            
            # Apply updates
            for i, row in enumerate(self._data[table_name]):
                if condition_func(row):
                    self._data[table_name][i] = update_func(row)
                    updated_count += 1
            
            if updated_count > 0:
                self._write_db()
            
            if self.verbose:
                print(f"Updated {updated_count} row(s) in '{table_name}'")
            
            return updated_count

    def delete(self, table_name: str, filters: Union[Dict[str, Any], Callable]) -> int:
        """
        Delete rows matching filters
        
        Args:
            table_name: Name of the table
            filters: Filter conditions (dict) or custom function (callable)
            
        Returns:
            Number of rows deleted
            
        Raises:
            TableNotFoundError: If table doesn't exist
            QueryError: If filters are invalid
        """
        with self._lock:
            if table_name not in self._data:
                raise TableNotFoundError(f"Table '{table_name}' does not exist")
            
            original_len = len(self._data[table_name])
            
            # Build condition function
            if callable(filters):
                condition_func = filters
            elif isinstance(filters, dict):
                condition_func = self._build_condition_function(filters)
            else:
                raise QueryError("Filters must be dict or callable")
            
            self._data[table_name] = [r for r in self._data[table_name] if not condition_func(r)]
            
            deleted_count = original_len - len(self._data[table_name])
            
            if deleted_count > 0:
                self._write_db()
            
            if self.verbose:
                print(f"Deleted {deleted_count} row(s) from '{table_name}'")
            
            return deleted_count

    def list_tables(self) -> List[str]:
        """
        Get list of all tables in database
        
        Returns:
            List of table names (sorted alphabetically)
        """
        return self._data["_meta"]["tables"].copy()

    def exists_table(self, table_name: str) -> bool:
        """
        Check if a table exists
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            True if table exists, False otherwise
        """
        return table_name in self._data
    
    def count(self, table_name: str, filters: Union[Dict[str, Any], Callable, None] = None) -> int:
        """
        Count rows matching filters
        
        Args:
            table_name: Name of the table
            filters: Filter conditions (dict) or custom function (callable)
            
        Returns:
            Number of matching rows
        """
        return len(self.select(table_name, filters))
    
    def stats(self, table_name: str) -> dict:
        """
        Get detailed statistics about a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with table statistics including:
            - total_rows: Total number of rows
            - fields: Statistics for each field
            - size_bytes: Estimated size in bytes
            - size_kb: Estimated size in kilobytes
            
        Raises:
            TableNotFoundError: If table doesn't exist
        """
        with self._lock:
            if table_name not in self._data:
                raise TableNotFoundError(f"Table '{table_name}' does not exist")
            
            rows = self._data[table_name]
            total_rows = len(rows)
            
            if total_rows == 0:
                return {
                    "table": table_name,
                    "total_rows": 0,
                    "fields": {},
                    "size_bytes": 0,
                    "size_kb": 0
                }
            
            # Collect all fields
            all_fields = set()
            for row in rows:
                all_fields.update(row.keys())
            
            # Calculate field statistics
            field_stats = {}
            for field in all_fields:
                values = [row.get(field) for row in rows if field in row]
                non_null = [v for v in values if v is not None]
                
                field_stats[field] = {
                    "present_count": len(values),
                    "present_percentage": round(len(values) / total_rows * 100, 2),
                    "unique_count": len(set(str(v) for v in non_null)),
                    "null_count": len(values) - len(non_null),
                    "data_types": list(set(type(v).__name__ for v in non_null))
                }
                
                # Add numeric stats if applicable
                if non_null and all(isinstance(v, (int, float)) for v in non_null):
                    field_stats[field]["min"] = min(non_null)
                    field_stats[field]["max"] = max(non_null)
                    field_stats[field]["avg"] = round(sum(non_null) / len(non_null), 2)
            
            # Estimate size
            size_bytes = len(json.dumps(rows).encode('utf-8'))
            
            return {
                "table": table_name,
                "total_rows": total_rows,
                "fields": field_stats,
                "size_bytes": size_bytes,
                "size_kb": round(size_bytes / 1024, 2)
            }
    
    def clear_table(self, table_name: str) -> int:
        """
        Remove all rows from table but keep the table structure
        
        Args:
            table_name: Name of the table to clear
            
        Returns:
            Number of rows removed
            
        Raises:
            TableNotFoundError: If table doesn't exist
        """
        with self._lock:
            if table_name not in self._data:
                raise TableNotFoundError(f"Table '{table_name}' does not exist")
            
            row_count = len(self._data[table_name])
            self._data[table_name] = []
            self._write_db()
            
            if self.verbose:
                print(f"Cleared {row_count} rows from '{table_name}'")
            
            return row_count
    
    def info(self) -> dict:
        """
        Get database information
        
        Returns:
            Dictionary with database metadata including:
            - path: Database file path
            - created: Creation timestamp
            - last_modified: Last modification timestamp
            - version: Database version
            - tables: Information about each table
            - table_count: Total number of tables
            - total_rows: Total rows across all tables
        """
        with self._lock:
            tables_info = {}
            for table in self.list_tables():
                tables_info[table] = {
                    "row_count": len(self._data[table])
                }
            
            return {
                "path": str(self.db_path),
                "created": self._data["_meta"].get("created"),
                "last_modified": self._data["_meta"].get("last_modified"),
                "version": self._data["_meta"].get("version"),
                "tables": tables_info,
                "table_count": len(self.list_tables()),
                "total_rows": sum(len(self._data[t]) for t in self.list_tables())
            }


if __name__ == "__main__":
    # Demo usage
    print("=== CafeDB Demo ===\n")
    
    db = CafeDB("demo.json", verbose=True)
    
    # Create table
    if not db.exists_table("users"):
        db.create_table("users")
    
    # Insert sample data
    sample_users = [
        {"name": "Alice Johnson", "age": 28, "city": "Paris", "email": "alice@gmail.com", "score": 85},
        {"name": "Bob Smith", "age": 34, "city": "London", "email": "bob@yahoo.com", "score": 72},
        {"name": "Anna Miller", "age": 22, "city": "Berlin", "email": "anna@gmail.com", "score": 91},
        {"name": "Charlie Brown", "age": 45, "city": "Paris", "email": "charlie@hotmail.com", "score": 68},
    ]
    
    db.insert_many("users", sample_users)
    
    print("\n=== Query Examples ===\n")
    
    # Wildcard matching
    print("1. Users with names starting with 'A':")
    results = db.select("users", {"name": "A*"})
    for user in results:
        print(f"   {user['name']} ({user['age']})")
    
    # String operations
    print("\n2. Gmail users:")
    results = db.select("users", {"email": "*@gmail.com"})
    for user in results:
        print(f"   {user['name']} - {user['email']}")
    
    # Range queries
    print("\n3. Users aged 25-35:")
    results = db.select("users", {"age": {"$between": [25, 35]}})
    for user in results:
        print(f"   {user['name']} - {user['age']} years old")
    
    # Sorting and pagination
    print("\n4. Top 2 users by score:")
    results = db.select("users", order_by="score", reverse=True, limit=2)
    for user in results:
        print(f"   {user['name']} - Score: {user['score']}")
    
    # Database info
    print("\n=== Database Info ===")
    info = db.info()
    print(f"Tables: {info['table_count']}, Total Rows: {info['total_rows']}")
    
    # Table statistics
    print("\n=== Table Statistics ===")
    stats = db.stats("users")
    print(f"Total rows: {stats['total_rows']}")
    print(f"Size: {stats['size_kb']} KB")
    print(f"\nField Statistics:")
    for field, info in stats['fields'].items():
        if not field.startswith('_'):
            print(f"  {field}: {info['unique_count']} unique values")