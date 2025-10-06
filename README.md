# CafeDB

A lightweight, human-readable JSON database for Python with zero dependencies and powerful querying capabilities.

[![Python 3.6+](https://img.shields.io/badge/python-3.6+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Pure Python](https://img.shields.io/badge/dependencies-none-brightgreen.svg)](https://github.com/yourusername/cafedb)

## Features

- **Zero Configuration** - Just point to a JSON/CDB file and start working
- **Human-Readable** - All data stored in plain JSON format
- **Advanced Querying** - Rich operator support including wildcards, regex, ranges, and logical operators ($or support)
- **Thread-Safe** - Built-in locking for concurrent operations
- **Atomic Writes** - Crash-safe operations with temporary files
- **Automatic Backups** - Optional backup creation on each write
- **Transaction Support** - Rollback capability for batch operations
- **Detailed Statistics** - Get insights about your data with extended field statistics
- **No Dependencies** - Pure Python with standard library only

## Installation

### Option 1: pip install
```bash
pip install cafedb
```

### Option 2: Copy the module
Simply copy `cafedb.py` to your project directory:
```python
from cafedb import CafeDB
```

### Option 3: Install from source
```bash
git clone https://github.com/Crystallinecore/cafedb.git
cd cafedb
pip install -e .
```

## Quick Start

```python
from cafedb import CafeDB

# Initialize database
db = CafeDB("mydata.cdb", verbose=True)

# Create a table
db.create_table("users")

# Insert data
db.insert("users", {
    "name": "Alice Johnson",
    "age": 28,
    "email": "alice@example.com",
    "city": "Paris"
})

# Batch insert
db.insert_many("users", [
    {"name": "Bob Smith", "age": 34, "email": "bob@example.com"},
    {"name": "Carol White", "age": 22, "email": "carol@example.com"}
])

# Query data with advanced operators
results = db.select("users", {"age": {"$gte": 25}})

# Sorting and pagination
top_users = db.select("users", order_by="age", reverse=True, limit=5)

# Update data
db.update("users", 
    {"name": "Alice Johnson"}, 
    {"city": "London"}
)

# Delete data
db.delete("users", {"age": {"$lt": 25}})
```

## Query Operators

CafeDB supports powerful query operators for filtering data:

### Comparison Operators

```python
# Equal
db.select("users", {"age": 28})
db.select("users", {"age": {"$eq": 28}})

# Not equal
db.select("users", {"age": {"$ne": 28}})

# Greater than / Less than
db.select("users", {"age": {"$gt": 25}})
db.select("users", {"age": {"$gte": 25}})
db.select("users", {"age": {"$lt": 30}})
db.select("users", {"age": {"$lte": 30}})

# Between (inclusive range)
db.select("users", {"age": {"$between": [25, 35]}})
```

### Membership Operators

```python
# In list
db.select("users", {"city": {"$in": ["Paris", "London", "Berlin"]}})

# Not in list
db.select("users", {"city": {"$nin": ["Paris", "London"]}})
```

### String Operators

```python
# Wildcard matching (* for any chars, ? for single char)
db.select("users", {"name": "A*"})  # Names starting with A
db.select("users", {"email": "*@gmail.com"})  # Gmail users

# Contains substring (case-insensitive)
db.select("users", {"bio": {"$contains": "python"}})

# Starts with / Ends with
db.select("users", {"name": {"$startswith": "Ali"}})
db.select("users", {"email": {"$endswith": ".com"}})

# Regex pattern matching
db.select("users", {"email": {"$regex": r"^[a-z]+@gmail\.com$"}})

# Like (wildcard shorthand)
db.select("users", {"name": {"$like": "J*son"}})

# Exists check
db.select("users", {"phone": {"$exists": True}})
```

### Logical Operators

```python
# Multiple conditions (AND by default)
db.select("users", {
    "age": {"$gte": 25},
    "city": "Paris",
    "email": "*@gmail.com"
})

# OR operator
db.select("users", {
    "$or": [
        {"city": "Paris"},
        {"city": "London"}
    ]
})

# Combining AND and OR
db.select("users", {
    "age": {"$gte": 25},  # AND condition
    "$or": [              # OR conditions
        {"city": "Paris"},
        {"score": {"$gte": 85}}
    ]
})
```

## Advanced Features

### Sorting and Pagination

```python
# Sort by field
results = db.select("users", order_by="age")

# Sort descending
results = db.select("users", order_by="score", reverse=True)

# Pagination
results = db.select("users", limit=10, offset=20)

# Combine all
results = db.select(
    "users",
    {"age": {"$gte": 25}},
    order_by="score",
    reverse=True,
    limit=5,
    offset=0
)
```

### Field Projection

```python
# Select specific fields only
results = db.select("users", fields=["name", "email"])
# Returns: [{"name": "Alice", "email": "alice@example.com"}, ...]
```

### Custom Filter Functions

```python
# Use a custom function for complex filtering
results = db.select("users", lambda row: row["age"] > 25 and "@gmail.com" in row["email"])

# Custom update function
db.update(
    "users",
    {"age": {"$gte": 30}},
    lambda row: {**row, "category": "senior", "discount": row["age"] * 0.01}
)
```

### Transactions

```python
# Rollback all changes if any operation fails
try:
    with db.transaction():
        db.insert("users", user1)
        db.update("users", {"name": "Alice"}, {"status": "active"})
        db.delete("users", {"status": "inactive"})
        # All changes committed together
except Exception as e:
    # All changes rolled back automatically
    print(f"Transaction failed: {e}")
```

### Batch Operations

```python
# Batch insert (more efficient)
users = [
    {"name": "User1", "age": 25},
    {"name": "User2", "age": 30},
    {"name": "User3", "age": 35}
]
count = db.insert_many("users", users)
print(f"Inserted {count} rows")

# Update multiple rows
count = db.update(
    "users",
    {"age": {"$gte": 30}},
    {"category": "senior"}
)
print(f"Updated {count} rows")

# Delete multiple rows
count = db.delete("users", {"age": {"$lt": 18}})
print(f"Deleted {count} rows")
```

## Utility Methods

### Table Management

```python
# List all tables
tables = db.list_tables()

# Check if table exists
if db.exists_table("users"):
    print("Users table exists")

# Clear all data from table (keep structure)
count = db.clear_table("users")
print(f"Cleared {count} rows")

# Drop table completely
db.drop_table("users")
```

### Statistics and Information

```python
# Count rows
total = db.count("users")
filtered = db.count("users", {"age": {"$gte": 25}})

# Detailed table statistics
stats = db.stats("users")
print(f"Total rows: {stats['total_rows']}")
print(f"Size: {stats['size_kb']} KB")
print(f"Fields: {stats['fields']}")

# Field statistics include:
# - present_count: Number of rows with this field
# - present_percentage: Percentage of rows with this field
# - unique_count: Number of unique values
# - null_count: Number of null values
# - data_types: List of data types found
# - min/max/avg: For numeric fields

# Database information
info = db.info()
print(f"Path: {info['path']}")
print(f"Tables: {info['table_count']}")
print(f"Total rows: {info['total_rows']}")
print(f"Created: {info['created']}")
print(f"Last modified: {info['last_modified']}")
```

## Configuration Options

```python
# Verbose mode (prints operations)
db = CafeDB("data.json", verbose=True)

# Disable automatic backups
db = CafeDB("data.json", backup=False)

# Combine options
db = CafeDB("data.json", verbose=True, backup=True)
```

## Error Handling

CafeDB uses custom exceptions for better error handling:

```python
from cafedb import (
    CafeDB, 
    CafeDBError,
    TableNotFoundError, 
    TableExistsError, 
    QueryError
)

try:
    db.create_table("users")
except TableExistsError:
    print("Table already exists")

try:
    db.select("nonexistent")
except TableNotFoundError as e:
    print(f"Error: {e}")

try:
    db.select("users", {"age": {"$invalid": 25}})
except QueryError as e:
    print(f"Invalid query: {e}")
```

## Data Structure

CafeDB stores data in a simple JSON format:

```json
{
  "_meta": {
    "tables": ["users", "products"],
    "created": "2025-01-15T10:30:00",
    "last_modified": "2025-01-15T11:45:00",
    "version": "1.0.0"
  },
  "users": [
    {
      "name": "Alice Johnson",
      "age": 28,
      "email": "alice@example.com",
      "_inserted_at": "2025-01-15T10:35:00",
      "_updated_at": "2025-01-15T11:00:00"
    }
  ]
}
```

## Best Practices

1. **Use batch operations** - `insert_many()` is more efficient than multiple `insert()` calls
2. **Enable backups for production** - Keep `backup=True` for important data
3. **Use transactions for related changes** - Group operations that should succeed or fail together
4. **Use field projection** - Only select the fields you need with the `fields` parameter
5. **Handle exceptions** - Always catch `TableNotFoundError`, `QueryError`, etc.
6. **Keep tables under 10,000 rows** - For optimal performance

## Limitations

- Not suitable for very large datasets (>100MB)
- No SQL-like JOIN operations (use application-level logic)
- No built-in indexing (all queries are full table scans)
- Single-file storage (one JSON file per database)
- Queries work on top-level fields only (no nested field queries)

## Use Cases

Perfect for:
- Small to medium-sized datasets
- Configuration storage
- Rapid prototyping
- Testing and development
- Embedded applications
- Data that needs to be human-readable
- Applications where simplicity is more important than performance

## API Reference

### Core Methods

- `create_table(table_name: str)` - Create a new table
- `drop_table(table_name: str)` - Delete a table
- `insert(table_name: str, row: dict)` - Insert a single row
- `insert_many(table_name: str, rows: List[dict])` - Batch insert rows
- `select(table_name: str, filters=None, fields=None, limit=None, offset=0, order_by=None, reverse=False)` - Query rows
- `update(table_name: str, filters, updater)` - Update matching rows
- `delete(table_name: str, filters)` - Delete matching rows
- `count(table_name: str, filters=None)` - Count matching rows
- `list_tables()` - List all tables
- `exists_table(table_name: str)` - Check if table exists
- `stats(table_name: str)` - Get table statistics
- `clear_table(table_name: str)` - Remove all rows from table
- `info()` - Get database information

### Context Managers

- `transaction()` - Execute operations with rollback support

## Complete Example

```python
from cafedb import CafeDB

# Initialize
db = CafeDB("app.json", verbose=True)

# Setup
if not db.exists_table("users"):
    db.create_table("users")

# Add users
db.insert_many("users", [
    {"username": "alice", "email": "alice@example.com", "age": 28, "role": "admin"},
    {"username": "bob", "email": "bob@example.com", "age": 34, "role": "user"},
    {"username": "carol", "email": "carol@example.com", "age": 22, "role": "user"}
])

# Find admin users
admins = db.select("users", {"role": "admin"})

# Find users by age range and role with OR logic
results = db.select("users", {
    "age": {"$between": [25, 35]},
    "$or": [
        {"role": "admin"},
        {"role": "moderator"}
    ]
})

# Update user email
db.update(
    "users",
    {"username": "alice"},
    {"email": "alice.new@example.com"}
)

# Promote users over 30
db.update(
    "users",
    {"age": {"$gte": 30}},
    {"role": "senior"}
)

# Get statistics with extended field info
stats = db.stats("users")
print(f"Total users: {stats['total_rows']}")
print(f"Database size: {stats['size_kb']} KB")
for field, info in stats['fields'].items():
    print(f"{field}: {info['unique_count']} unique, {info['present_percentage']}% present")

# Use transactions
try:
    with db.transaction():
        db.insert("users", {"username": "dave", "age": 40})
        db.update("users", {"role": "user"}, {"verified": True})
except Exception as e:
    print(f"Transaction failed: {e}")
```

## Contributing

Contributions are welcome! Feel free to:
- Report bugs
- Suggest features
- Submit pull requests
- Improve documentation

## License

MIT License - Free to use and modify for any purpose.

## FAQ

**Q: Can CafeDB handle millions of records?**  
A: CafeDB is optimized for small to medium datasets (< 100K records). For larger datasets, consider a traditional database.

**Q: Is CafeDB thread-safe?**  
A: Yes, CafeDB uses `threading.RLock()` for thread-safe operations.

**Q: Can I use CafeDB in production?**  
A: CafeDB is great for prototypes, small applications, and internal tools. For mission-critical production systems with high load, use PostgreSQL, MongoDB, etc.

**Q: How do I backup my database?**  
A: CafeDB creates automatic backups (if enabled). You can also manually copy the JSON file.

**Q: What about JOIN operations?**  
A: CafeDB doesn't support JOINs. Query multiple tables and combine results in your application code.

---

**☕ CafeDB** - Simple, powerful, human-readable database for Python.
