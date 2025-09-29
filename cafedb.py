import json
import re
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Callable, Union


class CafeDB:
    def __init__(self, db_path: str, verbose: bool = False):
        self.db_path = Path(db_path)
        self.verbose = verbose
        if self.db_path.exists():
            self._data = self._read_db()
        else:
            self._data = {"_meta": {"tables": [], "created": datetime.now().isoformat()}}
            self._write_db()
        if "_meta" not in self._data:
            self._data["_meta"] = {"tables": [], "created": datetime.now().isoformat()}

    def _read_db(self):
        with open(self.db_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _write_db(self):
        tmp_path = self.db_path.with_suffix(self.db_path.suffix + ".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2)
        tmp_path.replace(self.db_path)
        if self.verbose:
            print(f"DB written to {self.db_path}")

    def create_table(self, table_name: str):
        if table_name in self._data:
            raise ValueError(f"Table '{table_name}' already exists.")
        self._data[table_name] = []
        self._data["_meta"]["tables"].append(table_name)
        self._write_db()
        if self.verbose:
            print(f"Table '{table_name}' created.")

    def drop_table(self, table_name: str):
        if table_name not in self._data:
            raise ValueError(f"Table '{table_name}' does not exist.")
        del self._data[table_name]
        self._data["_meta"]["tables"].remove(table_name)
        self._write_db()
        if self.verbose:
            print(f"Table '{table_name}' deleted.")

    def _match_wildcard(self, value: Any, pattern: str) -> bool:
        if not isinstance(value, str):
            value = str(value)
        
        regex_pattern = pattern.replace('*', '.*').replace('?', '.')
        regex_pattern = f"^{regex_pattern}$"
        
        return bool(re.match(regex_pattern, value, re.IGNORECASE))
    
    def _match_condition(self, value: Any, condition: Union[Any, Dict[str, Any]]) -> bool:
        if isinstance(condition, dict):
            for op, op_value in condition.items():
                if op == '$eq':  
                    if value != op_value:
                        return False
                elif op == '$ne': 
                    if value == op_value:
                        return False
                elif op == '$gt':  
                    if not (value > op_value):
                        return False
                elif op == '$gte':  
                    if not (value >= op_value):
                        return False
                elif op == '$lt':  
                    if not (value < op_value):
                        return False
                elif op == '$lte': 
                    if not (value <= op_value):
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
                    if not re.search(op_value, str(value), re.IGNORECASE):
                        return False
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
                    if len(op_value) != 2:
                        raise ValueError("$between requires array of exactly 2 values")
                    min_val, max_val = op_value
                    if not (min_val <= value <= max_val):
                        return False
                else:
                    raise ValueError(f"Unknown operator: {op}")
            return True
        else:
            if isinstance(condition, str) and ('*' in condition or '?' in condition):
                return self._match_wildcard(value, condition)
            else:
                return value == condition
    
    def _build_condition_function(self, filters: Dict[str, Any]) -> Callable:
        def condition_func(row: dict) -> bool:
            for field, condition in filters.items():
                if field not in row:
                    return False
                
                if not self._match_condition(row[field], condition):
                    return False
            
            return True
        
        return condition_func

    def insert(self, table_name: str, row: dict):
        if table_name not in self._data:
            raise ValueError(f"Table '{table_name}' does not exist.")
        self._data[table_name].append(row)
        self._write_db()
        if self.verbose:
            print(f"Inserted row into '{table_name}': {row}")

    def select(self, table_name: str, filters: Union[Dict[str, Any], Callable] = None):
        if table_name not in self._data:
            raise ValueError(f"Table '{table_name}' does not exist.")
        
        rows = self._data[table_name]
        
        if filters is None:
            return rows.copy()
        
        if callable(filters):
            return [r for r in rows if filters(r)]
        
        if isinstance(filters, dict):
            condition_func = self._build_condition_function(filters)
            return [r for r in rows if condition_func(r)]
        
        raise ValueError("Filters must be dict, callable, or None")

    def update(self, table_name: str, filters: Union[Dict[str, Any], Callable], updater: Union[Dict[str, Any], Callable]):
        if table_name not in self._data:
            raise ValueError(f"Table '{table_name}' does not exist.")
        
        updated_count = 0
        
        if callable(filters):
            condition_func = filters
        elif isinstance(filters, dict):
            condition_func = self._build_condition_function(filters)
        else:
            raise ValueError("Filters must be dict or callable")
        
        if callable(updater):
            update_func = updater
        elif isinstance(updater, dict):
            update_func = lambda row: {**row, **updater}
        else:
            raise ValueError("Updater must be dict or callable")
        
        for i, row in enumerate(self._data[table_name]):
            if condition_func(row):
                self._data[table_name][i] = update_func(row)
                updated_count += 1
        
        self._write_db()
        if self.verbose:
            print(f"Updated {updated_count} row(s) in '{table_name}'.")
        
        return updated_count

    def delete(self, table_name: str, filters: Union[Dict[str, Any], Callable]):
        if table_name not in self._data:
            raise ValueError(f"Table '{table_name}' does not exist.")
        
        original_len = len(self._data[table_name])
        
        # Build condition function
        if callable(filters):
            condition_func = filters
        elif isinstance(filters, dict):
            condition_func = self._build_condition_function(filters)
        else:
            raise ValueError("Filters must be dict or callable")
        
        self._data[table_name] = [r for r in self._data[table_name] if not condition_func(r)]
        
        deleted_count = original_len - len(self._data[table_name])
        self._write_db()
        if self.verbose:
            print(f"Deleted {deleted_count} row(s) from '{table_name}'.")
        
        return deleted_count

    def list_tables(self):
        return self._data["_meta"]["tables"].copy()

    def exists_table(self, table_name: str):
        return table_name in self._data
    
    def count(self, table_name: str, filters: Union[Dict[str, Any], Callable] = None):
        return len(self.select(table_name, filters))
    
    def stats(self, table_name: str):
        if table_name not in self._data:
            raise ValueError(f"Table '{table_name}' does not exist.")
        
        rows = self._data[table_name]
        total_rows = len(rows)
        
        if total_rows == 0:
            return {"table": table_name, "total_rows": 0, "fields": []}
        
        all_fields = set()
        for row in rows:
            all_fields.update(row.keys())
        
        field_stats = {}
        for field in all_fields:
            values = [row.get(field) for row in rows if field in row]
            field_stats[field] = {
                "present_count": len(values),
                "unique_count": len(set(str(v) for v in values if v is not None)),
                "null_count": sum(1 for v in values if v is None),
                "data_types": list(set(type(v).__name__ for v in values if v is not None))
            }
        
        return {
            "table": table_name,
            "total_rows": total_rows,
            "fields": field_stats
        }

if __name__ == "__main__":
    db = CafeDB("test_enhanced.cdb", verbose=True)
    
    try:
        db.create_table("users")
    except ValueError:
        pass  

    sample_users = [
        {"name": "Alice Johnson", "age": 28, "city": "Paris", "email": "alice@gmail.com", "score": 85, "bio": "Python developer"},
        {"name": "Bob Smith", "age": 34, "city": "London", "email": "bob@yahoo.com", "score": 72, "bio": "Java programmer"},
        {"name": "Anna Miller", "age": 22, "city": "Berlin", "email": "anna@gmail.com", "score": 91, "bio": "Data scientist with Python"},
        {"name": "Charlie Brown", "age": 45, "city": "Paris", "email": "charlie@hotmail.com", "score": 68, "bio": "Manager and team lead"},
        {"name": "Amy Wilson", "age": 31, "city": "London", "email": "amy@gmail.com", "score": 88, "bio": "Frontend developer"},
    ]
    
    for user in sample_users:
        try:
            db.insert("users", user)
        except:
            pass 
    print("\n=== ENHANCED FILTERING EXAMPLES ===\n")
    
    print("1. Users with names starting with 'A':")
    results = db.select("users", {"name": "A*"})
    for user in results:
        print(f"   {user['name']} ({user['age']})")
    
    print("\n2. Gmail users:")
    results = db.select("users", {"email": "*@gmail.com"})
    for user in results:
        print(f"   {user['name']} - {user['email']}")
    
    print("\n3. Users aged 25-35:")
    results = db.select("users", {"age": {"$between": [25, 35]}})
    for user in results:
        print(f"   {user['name']} - {user['age']} years old")
    
    print("\n4. High performers (score >= 80):")
    results = db.select("users", {"score": {"$gte": 80}})
    for user in results:
        print(f"   {user['name']} - Score: {user['score']}")
    
    print("\n5. Python developers:")
    results = db.select("users", {"bio": {"$contains": "python"}})
    for user in results:
        print(f"   {user['name']} - {user['bio']}")
    
    print("\n6. Young Gmail users in major cities:")
    results = db.select("users", {
        "age": {"$lt": 30},
        "email": "*@gmail.com",
        "city": {"$in": ["Paris", "London", "Berlin"]}
    })
    for user in results:
        print(f"   {user['name']} ({user['age']}) in {user['city']}")
    
    print("\n7. Updating senior users (45+):")
    updated = db.update("users", 
        {"age": {"$gte": 45}},
        {"category": "senior", "discount": 0.1}
    )
    
    print("\n=== TABLE STATISTICS ===")
    stats = db.stats("users")
    print(f"Table: {stats['table']}")
    print(f"Total rows: {stats['total_rows']}")
    print("Fields:")
    for field, info in stats['fields'].items():
        print(f"  {field}: {info['present_count']} present, {info['unique_count']} unique, types: {info['data_types']}")