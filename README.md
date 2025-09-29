# CafeDB - Enhanced JSON Database

A simple, lightweight, and powerful Python database that stores data in human-readable JSON format with advanced querying capabilities.

[![Python 3.6+](https://img.shields.io/badge/python-3.6+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Pure Python](https://img.shields.io/badge/dependencies-none-brightgreen.svg)](https://github.com/yourusername/cafedb)

## 🚀 Quick Start

```python
from cafedb import CafeDB

# Create or open database
db = CafeDB("my_database.json", verbose=True)

# Create table
db.create_table("users")

# Insert data
db.insert("users", {
    "name": "Alice Johnson",
    "age": 28,
    "city": "Paris",
    "email": "alice@gmail.com",
    "skills": ["Python", "JavaScript", "SQL"]
})

# Simple queries
all_users = db.select("users")
paris_users = db.select("users", {"city": "Paris"})

# Advanced queries with wildcards and comparisons
gmail_users = db.select("users", {"email": "*@gmail.com"})
young_adults = db.select("users", {"age": {"$between": [18, 30]}})
developers = db.select("users", {"skills": {"$contains": "Python"}})
```

## 📋 Table of Contents

- [Installation](#installation)
- [Core Features](#core-features)
- [Basic Operations](#basic-operations)
- [Advanced Querying](#advanced-querying)
- [Query Operators](#query-operators)
- [Data Types](#data-types)
- [Performance Guidelines](#performance-guidelines)
- [Best Practices](#best-practices)
- [Error Handling](#error-handling)
- [Examples](#examples)
- [API Reference](#api-reference)
- [Migration Guide](#migration-guide)
- [Contributing](#contributing)
- [License](#license)

## 💾 Installation

### Option 1: Copy the Module
Download `cafedb.py` and place it in your project directory:

```python
from cafedb import CafeDB
```

### Option 2: Install as Package
```bash
# Clone repository
git clone https://github.com/yourusername/cafedb.git
cd cafedb

# Install in development mode
pip install -e .
```

### Requirements
- Python 3.6+
- No external dependencies (pure Python!)

## ✨ Core Features

### 🎯 **Simple & Intuitive**
- Human-readable JSON storage format
- Pythonic API that feels natural
- Zero configuration required
- Single file database

### ⚡ **Advanced Querying**
- Wildcard pattern matching (`*`, `?`)
- Comparison operators (`$gt`, `$lt`, `$between`, etc.)
- String operations (`$contains`, `$startswith`, `$endswith`)
- Regular expression matching
- List membership testing (`$in`, `$nin`)

### 🔧 **Developer Friendly**
- Verbose mode for debugging
- Atomic file operations (crash-safe)
- Detailed error messages
- Built-in statistics and introspection

### 📈 **Production Ready**
- Automatic backup creation during writes
- Table metadata management
- Data validation and type checking
- Thread-safe operations

## 🛠️ Basic Operations

### Database Management

```python
from cafedb import CafeDB

# Initialize database
db = CafeDB("myapp.json", verbose=True)

# List all tables
tables = db.list_tables()
print(f"Tables: {tables}")

# Check if table exists
if db.exists_table("users"):
    print("Users table exists")

# Get database statistics
stats = db.stats("users")
print(f"Total users: {stats['total_rows']}")
```

### Table Operations

```python
# Create table
db.create_table("products")

# Drop table (be careful!)
db.drop_table("old_table")

# Create table only if it doesn't exist
if not db.exists_table("logs"):
    db.create_table("logs")
```

### CRUD Operations

#### Insert Data
```python
# Insert single record
db.insert("users", {
    "id": 1,
    "name": "Alice Johnson",
    "age": 28,
    "city": "Paris",
    "email": "alice@example.com",
    "active": True,
    "tags": ["developer", "python", "remote"]
})

# Insert multiple records
users_data = [
    {"name": "Bob Smith", "age": 34, "city": "London"},
    {"name": "Carol Davis", "age": 29, "city": "Berlin"},
    {"name": "David Wilson", "age": 31, "city": "Paris"}
]

for user in users_data:
    db.insert("users", user)
```

#### Select Data
```python
# Get all records
all_users = db.select("users")

# Get specific records with filters
paris_users = db.select("users", {"city": "Paris"})

# Count records
total_users = db.count("users")
active_users = db.count("users", {"active": True})
```

#### Update Data
```python
# Update with dictionary filters and updates
db.update("users", 
    {"city": "Paris"}, 
    {"timezone": "CET", "updated_at": "2024-01-01"}
)

# Update with custom function
db.update("users",
    {"age": {"$gte": 30}},
    lambda user: {**user, "category": "senior", "discount": 0.1}
)

# Get update count
updated = db.update("users", {"active": False}, {"status": "inactive"})
print(f"Updated {updated} inactive users")
```

#### Delete Data
```python
# Delete with filters
deleted = db.delete("users", {"active": False})
print(f"Deleted {deleted} inactive users")

# Delete with complex conditions
db.delete("users", {
    "age": {"$lt": 18},
    "verified": False
})
```

## 🔍 Advanced Querying

### Wildcard Pattern Matching

```python
# Names starting with 'A'
a_names = db.select("users", {"name": "A*"})

# Gmail users
gmail_users = db.select("users", {"email": "*@gmail.com"})

# Files with specific extension
pdf_files = db.select("documents", {"filename": "*.pdf"})

# Single character wildcard
codes = db.select("products", {"code": "US?"})  # US1, US2, USA, etc.

# Multiple wildcards
patterns = db.select("logs", {"message": "*error*database*"})
```

### Comparison Operators

```python
# Age ranges
adults = db.select("users", {"age": {"$gte": 18}})
seniors = db.select("users", {"age": {"$between": [65, 100]}})
young_adults = db.select("users", {"age": {"$gte": 18, "$lt": 35}})

# Price comparisons
expensive = db.select("products", {"price": {"$gt": 100}})
on_sale = db.select("products", {"discount": {"$ne": 0}})

# Date ranges (works with ISO date strings)
recent = db.select("orders", {
    "created_at": {"$gte": "2024-01-01T00:00:00Z"}
})
```

### String Operations

```python
# Case-insensitive contains
python_devs = db.select("users", {"bio": {"$contains": "python"}})
remote_workers = db.select("users", {"description": {"$contains": "remote"}})

# String starts/ends with
mr_users = db.select("users", {"name": {"$startswith": "Mr."}})
com_emails = db.select("users", {"email": {"$endswith": ".com"}})

# Regular expressions
phone_pattern = db.select("users", {
    "phone": {"$regex": r"\(\d{3}\) \d{3}-\d{4}"}
})

us_phones = db.select("contacts", {
    "phone": {"$regex": r"^\+1"}
})
```

### List Operations

```python
# Value in list
major_cities = db.select("users", {
    "city": {"$in": ["New York", "London", "Paris", "Tokyo"]}
})

# Value not in list
active_users = db.select("users", {
    "status": {"$nin": ["banned", "suspended", "deleted"]}
})

# Multiple list conditions
qualified = db.select("candidates", {
    "skills": {"$contains": "Python"},
    "location": {"$in": ["Remote", "New York", "San Francisco"]},
    "experience": {"$gte": 2}
})
```

### Complex Multi-Field Queries

```python
# Combine multiple condition types
premium_users = db.select("users", {
    "name": "A*",                              # Wildcard
    "age": {"$between": [25, 45]},            # Range
    "city": {"$in": ["Paris", "London"]},     # List membership
    "email": "*@gmail.com",                   # Wildcard
    "bio": {"$contains": "developer"},        # String contains
    "score": {"$gte": 80},                    # Comparison
    "active": True                            # Exact match
})

# Complex business logic
target_customers = db.select("customers", {
    "age": {"$between": [25, 55]},
    "income": {"$gte": 50000},
    "location": {"$nin": ["Rural"]},
    "last_purchase": {"$gte": "2023-01-01"},
    "email": {"$endswith": ".com"},
    "preferences": {"$contains": "premium"}
})
```

## 📊 Query Operators Reference

### Comparison Operators

| Operator | Description | Example | SQL Equivalent |
|----------|-------------|---------|----------------|
| `$eq` | Equal to | `{"age": {"$eq": 30}}` | `age = 30` |
| `$ne` | Not equal to | `{"status": {"$ne": "inactive"}}` | `status != 'inactive'` |
| `$gt` | Greater than | `{"score": {"$gt": 80}}` | `score > 80` |
| `$gte` | Greater than or equal | `{"age": {"$gte": 18}}` | `age >= 18` |
| `$lt` | Less than | `{"price": {"$lt": 100}}` | `price < 100` |
| `$lte` | Less than or equal | `{"discount": {"$lte": 0.5}}` | `discount <= 0.5` |
| `$between` | Between (inclusive) | `{"age": {"$between": [18, 65]}}` | `age BETWEEN 18 AND 65` |

### List Operators

| Operator | Description | Example | SQL Equivalent |
|----------|-------------|---------|----------------|
| `$in` | Value in list | `{"city": {"$in": ["Paris", "London"]}}` | `city IN ('Paris', 'London')` |
| `$nin` | Value not in list | `{"status": {"$nin": ["banned"]}}` | `status NOT IN ('banned')` |

### String Operators

| Operator | Description | Example | SQL Equivalent |
|----------|-------------|---------|----------------|
| `$like` | Wildcard matching | `{"name": {"$like": "A*"}}` | `name LIKE 'A%'` |
| `$regex` | Regular expression | `{"email": {"$regex": ".*@gmail\\.com"}}` | `email REGEXP '.*@gmail\.com'` |
| `$contains` | String contains (case-insensitive) | `{"bio": {"$contains": "python"}}` | `LOWER(bio) LIKE '%python%'` |
| `$startswith` | String starts with | `{"name": {"$startswith": "Dr."}}` | `name LIKE 'Dr.%'` |
| `$endswith` | String ends with | `{"email": {"$endswith": ".edu"}}` | `email LIKE '%.edu'` |

### Wildcard Patterns

| Pattern | Matches | Example |
|---------|---------|---------|
| `*` | Any sequence of characters | `"A*"` matches "Alice", "Andrew", "A" |
| `?` | Single character | `"A?"` matches "Al", "A1", but not "Alice" |
| `*word*` | Contains word | `"*dev*"` matches "developer", "development" |
| `prefix*` | Starts with prefix | `"Dr.*"` matches "Dr. Smith", "Dr. Jones" |
| `*suffix` | Ends with suffix | `"*.pdf"` matches "report.pdf", "doc.pdf" |

## 🗃️ Data Types Support

CafeDB supports all JSON-serializable Python data types:

### Primitive Types
```python
db.insert("mixed_data", {
    "string_field": "Hello World",
    "integer_field": 42,
    "float_field": 3.14159,
    "boolean_field": True,
    "null_field": None
})
```

### Collections
```python
db.insert("collections", {
    "list_field": [1, 2, 3, "mixed", True],
    "dict_field": {
        "nested_string": "value",
        "nested_number": 100,
        "deep_nest": {
            "level2": "deep value"
        }
    }
})
```

### Date and Time
```python
from datetime import datetime

db.insert("events", {
    "name": "Conference",
    "start_date": "2024-03-15",
    "start_time": "09:00:00",
    "created_at": datetime.now().isoformat(),
    "timestamp": 1640995200  # Unix timestamp
})

# Query dates (as strings)
recent_events = db.select("events", {
    "start_date": {"$gte": "2024-01-01"}
})
```

### Complex Nested Data
```python
db.insert("users", {
    "name": "Alice Johnson",
    "profile": {
        "personal": {
            "age": 28,
            "location": "Paris"
        },
        "professional": {
            "title": "Senior Developer",
            "skills": ["Python", "JavaScript", "Docker"],
            "experience": 5
        }
    },
    "preferences": {
        "notifications": True,
        "theme": "dark",
        "languages": ["en", "fr"]
    }
})

# Note: Queries work on top-level fields only
# For nested data, store flattened versions or use custom logic
```

## 🚀 Performance Guidelines

### Optimization Tips

1. **Keep Tables Reasonably Sized**
   ```python
   # Good: < 10,000 records per table
   # Acceptable: 10,000 - 100,000 records
   # Consider alternatives: > 100,000 records
   ```

2. **Use Appropriate Data Types**
   ```python
   # Good: Use numbers for numeric comparisons
   {"age": 25, "score": 85.5}
   
   # Less efficient: String numbers
   {"age": "25", "score": "85.5"}
   ```

3. **Optimize Query Patterns**
   ```python
   # Efficient: Exact matches and simple conditions
   db.select("users", {"city": "Paris", "active": True})
   
   # Less efficient: Complex regex on large datasets
   db.select("users", {"bio": {"$regex": ".*complex.*pattern.*"}})
   ```

4. **Batch Operations**
   ```python
   # Efficient: Batch inserts
   for record in large_dataset:
       db.insert("table", record)
   
   # Consider: Disable verbose mode for bulk operations
   db.verbose = False
   # ... bulk operations ...
   db.verbose = True
   ```

### Performance Benchmarks

Typical performance on modern hardware:

| Operation | Small Table (<1K) | Medium Table (10K) | Large Table (100K) |
|-----------|-------------------|-------------------|-------------------|
| Insert | <1ms | <1ms | 1-5ms |
| Simple Select | <1ms | 10-50ms | 100-500ms |
| Complex Query | 1-5ms | 50-200ms | 500ms-2s |
| Update | 1-10ms | 50-200ms | 500ms-2s |
| Delete | 1-10ms | 50-200ms | 500ms-2s |

## 💡 Best Practices

### Database Design

1. **Table Structure**
   ```python
   # Good: Logical table separation
   db.create_table("users")
   db.create_table("orders") 
   db.create_table("products")
   
   # Avoid: Everything in one table
   db.create_table("everything")
   ```

2. **Field Naming**
   ```python
   # Good: Consistent, descriptive names
   {
       "user_id": 123,
       "created_at": "2024-01-01T00:00:00Z",
       "is_active": True
   }
   
   # Avoid: Inconsistent naming
   {
       "id": 123,
       "CreatedDate": "2024-01-01",
       "active": 1
   }
   ```

3. **Data Consistency**
   ```python
   # Good: Consistent data types
   {"age": 25, "score": 85}
   
   # Avoid: Mixed types for same field
   {"age": "25", "score": 85}  # age as string, score as number
   ```

### Query Optimization

1. **Use Specific Filters**
   ```python
   # Good: Specific conditions
   db.select("users", {"city": "Paris", "active": True})
   
   # Less efficient: Broad patterns
   db.select("users", {"name": "*"})  # Returns everyone
   ```

2. **Combine Conditions Effectively**
   ```python
   # Good: Multiple specific conditions (AND logic)
   db.select("products", {
       "category": "Electronics",
       "price": {"$between": [100, 500]},
       "in_stock": True
   })
   ```

3. **Use Appropriate Operators**
   ```python
   # Good: Use $between for ranges
   {"age": {"$between": [18, 65]}}
   
   # Less efficient: Multiple conditions
   {"age": {"$gte": 18, "$lte": 65}}
   ```

### Error Prevention

1. **Always Check Table Existence**
   ```python
   if not db.exists_table("users"):
       db.create_table("users")
   
   # Or use try/except
   try:
       db.create_table("users")
   except ValueError:
       pass  # Table already exists
   ```

2. **Validate Data Before Insert**
   ```python
   def validate_user(user_data):
       required_fields = ["name", "email"]
       for field in required_fields:
           if field not in user_data:
               raise ValueError(f"Missing required field: {field}")
       return user_data
   
   # Use validation
   user = validate_user({"name": "Alice", "email": "alice@example.com"})
   db.insert("users", user)
   ```

3. **Handle Exceptions Gracefully**
   ```python
   try:
       results = db.select("users", {"invalid_field": {"$unknown": "value"}})
   except ValueError as e:
       print(f"Query error: {e}")
       # Handle error appropriately
   ```

## ⚠️ Error Handling

### Common Exceptions

```python
from cafedb import CafeDB

db = CafeDB("test.json")

# Table doesn't exist
try:
    db.select("nonexistent_table")
except ValueError as e:
    print(f"Error: {e}")  # "Table 'nonexistent_table' does not exist."

# Table already exists
try:
    db.create_table("users")
    db.create_table("users")  # Will fail
except ValueError as e:
    print(f"Error: {e}")  # "Table 'users' already exists."

# Invalid operator
try:
    db.select("users", {"age": {"$invalid": 25}})
except ValueError as e:
    print(f"Error: {e}")  # "Unknown operator: $invalid"

# Invalid $between format
try:
    db.select("users", {"age": {"$between": [25]}})  # Need 2 values
except ValueError as e:
    print(f"Error: {e}")  # "$between requires array of exactly 2 values"
```

### Error Recovery

```python
# Database file corruption recovery
try:
    db = CafeDB("corrupted.json")
except json.JSONDecodeError:
    print("Database file corrupted, creating new one")
    # Backup corrupted file
    import shutil
    shutil.copy("corrupted.json", "corrupted.json.backup")
    
    # Create fresh database
    Path("corrupted.json").unlink()
    db = CafeDB("corrupted.json")

# Graceful degradation
def safe_query(db, table, filters):
    try:
        return db.select(table, filters)
    except ValueError as e:
        print(f"Query failed: {e}")
        return []  # Return empty results instead of crashing

results = safe_query(db, "users", {"age": {"$gte": 18}})
```

## 📚 Examples

### Example 1: User Management System

```python
from cafedb import CafeDB
from datetime import datetime

# Initialize database
db = CafeDB("user_management.json", verbose=True)
db.create_table("users")
db.create_table("sessions")

# User registration
def register_user(name, email, age, city):
    # Check if user already exists
    existing = db.select("users", {"email": email})
    if existing:
        raise ValueError("User with this email already exists")
    
    user = {
        "name": name,
        "email": email,
        "age": age,
        "city": city,
        "created_at": datetime.now().isoformat(),
        "active": True,
        "login_count": 0
    }
    
    db.insert("users", user)
    return user

# User authentication simulation
def login_user(email):
    users = db.select("users", {"email": email, "active": True})
    if not users:
        raise ValueError("User not found or inactive")
    
    user = users[0]
    
    # Update login count
    db.update("users", 
        {"email": email},
        lambda u: {**u, "login_count": u.get("login_count", 0) + 1, 
                  "last_login": datetime.now().isoformat()}
    )
    
    # Create session
    session = {
        "email": email,
        "login_time": datetime.now().isoformat(),
        "active": True
    }
    db.insert("sessions", session)
    
    return user

# Analytics
def get_user_analytics():
    total_users = db.count("users")
    active_users = db.count("users", {"active": True})
    recent_users = db.count("users", {
        "created_at": {"$gte": "2024-01-01T00:00:00Z"}
    })
    
    # Most common cities
    all_users = db.select("users")
    city_count = {}
    for user in all_users:
        city = user.get("city", "Unknown")
        city_count[city] = city_count.get(city, 0) + 1
    
    return {
        "total_users": total_users,
        "active_users": active_users,
        "recent_users": recent_users,
        "cities": dict(sorted(city_count.items(), key=lambda x: x[1], reverse=True))
    }

# Usage
register_user("Alice Johnson", "alice@example.com", 28, "Paris")
register_user("Bob Smith", "bob@example.com", 34, "London")

user = login_user("alice@example.com")
print(f"Welcome back, {user['name']}!")

analytics = get_user_analytics()
print(f"Analytics: {analytics}")
```

### Example 2: E-commerce Product Catalog

```python
from cafedb import CafeDB
import uuid

db = CafeDB("ecommerce.json", verbose=True)
db.create_table("products")
db.create_table("orders")

# Product management
def add_product(name, category, price, description, tags):
    product = {
        "id": str(uuid.uuid4()),
        "name": name,
        "category": category,
        "price": price,
        "description": description,
        "tags": tags,
        "in_stock": True,
        "created_at": datetime.now().isoformat()
    }
    
    db.insert("products", product)
    return product

# Advanced product search
def search_products(query=None, category=None, min_price=None, max_price=None, tags=None):
    filters = {}
    
    # Text search in name and description
    if query:
        # This is a limitation - we'd need to search both fields
        # For now, search in name only
        filters["name"] = {"$contains": query}
    
    if category:
        filters["category"] = category
    
    if min_price is not None and max_price is not None:
        filters["price"] = {"$between": [min_price, max_price]}
    elif min_price is not None:
        filters["price"] = {"$gte": min_price}
    elif max_price is not None:
        filters["price"] = {"$lte": max_price}
    
    if tags:
        # For simplicity, check if any tag matches
        # In a real system, you might store tags as separate records
        for tag in tags:
            filters[f"tags"] = {"$contains": tag}
    
    filters["in_stock"] = True
    
    return db.select("products", filters)

# Order management
def create_order(customer_email, product_ids):
    # Validate products exist
    order_items = []
    total_amount = 0
    
    for product_id in product_ids:
        products = db.select("products", {"id": product_id, "in_stock": True})
        if not products:
            raise ValueError(f"Product {product_id} not found or out of stock")
        
        product = products[0]
        order_items.append({
            "product_id": product_id,
            "name": product["name"],
            "price": product["price"]
        })
        total_amount += product["price"]
    
    order = {
        "id": str(uuid.uuid4()),
        "customer_email": customer_email,
        "items": order_items,
        "total_amount": total_amount,
        "status": "pending",
        "created_at": datetime.now().isoformat()
    }
    
    db.insert("orders", order)
    return order

# Analytics
def get_sales_report():
    orders = db.select("orders", {"status": {"$ne": "cancelled"}})
    
    total_orders = len(orders)
    total_revenue = sum(order["total_amount"] for order in orders)
    avg_order_value = total_revenue / total_orders if total_orders > 0 else 0
    
    return {
        "total_orders": total_orders,
        "total_revenue": total_revenue,
        "average_order_value": avg_order_value
    }

# Usage
add_product("Laptop Pro", "Electronics", 1299.99, "High-performance laptop", ["laptop", "computer", "electronics"])
add_product("Wireless Mouse", "Electronics", 29.99, "Ergonomic wireless mouse", ["mouse", "wireless", "accessories"])
add_product("Coffee Mug", "Home", 12.99, "Ceramic coffee mug", ["mug", "coffee", "ceramic"])

# Search examples
electronics = search_products(category="Electronics")
affordable = search_products(max_price=50)
laptops = search_products(query="laptop")

print(f"Found {len(electronics)} electronics products")
print(f"Found {len(affordable)} affordable products")
```

### Example 3: Log Analysis System

```python
from cafedb import CafeDB
import re
from datetime import datetime, timedelta

db = CafeDB("logs.json", verbose=True)
db.create_table("access_logs")
db.create_table("error_logs")

# Log parsing and storage
def parse_and_store_log_line(log_line, log_type="access"):
    # Example log line: "2024-01-01 10:30:45 [INFO] User alice@example.com accessed /dashboard"
    pattern = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(\w+)\] (.+)"
    match = re.match(pattern, log_line)
    
    if not match:
        return None
    
    timestamp, level, message = match.groups()
    
    log_entry = {
        "timestamp": timestamp,
        "level": level,
        "message": message,
        "parsed_at": datetime.now().isoformat()
    }
    
    # Extract additional info based on log type
    if log_type == "access":
        # Try to extract user and endpoint
        user_match = re.search(r"User (\S+) accessed (\S+)", message)
        if user_match:
            log_entry["user"] = user_match.group(1)
            log_entry["endpoint"] = user_match.group(2)
        
        db.insert("access_logs", log_entry)
    
    elif log_type == "error":
        # Try to extract error type
        if "database" in message.lower():
            log_entry["error_type"] = "database"
        elif "network" in message.lower():
            log_entry["error_type"] = "network"
        else:
            log_entry["error_type"] = "general"
        
        db.insert("error_logs", log_entry)
    
    return log_entry

# Log analysis functions
def analyze_access_patterns():
    # Most active users
    logs = db.select("access_logs")
    user_counts = {}
    endpoint_counts = {}
    
    for log in logs:
        user = log.get("user", "unknown")
        endpoint = log.get("endpoint", "unknown")
        
        user_counts[user] = user_counts.get(user, 0) + 1
        endpoint_counts[endpoint] = endpoint_counts.get(endpoint, 0) + 1
    
    return {
        "top_users": sorted(user_counts.items(), key=lambda x: x[1], reverse=True)[:10],
        "top_endpoints": sorted(endpoint_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    }

def find_errors_in_timeframe(start_time, end_time):
    # Find errors in specific time range
    return db.select("error_logs", {
        "timestamp": {"$gte": start_time, "$lte": end_time}
    })

def get_error_summary():
    # Get error breakdown by type
    errors = db.select("error_logs")
    error_types = {}
    levels = {}
    
    for error in errors:
        error_type = error.get("error_type", "unknown")
        level = error.get("level", "unknown")
        
        error_types[error_type] = error_types.get(error_type, 0) + 1
        levels[level] = levels.get(level, 0) + 1
    
    return {
        "total_errors": len(errors),
        "by_type": error_types,
        "by_level": levels
    }

# Alert system
def check_for_critical_errors():
    # Find critical errors in last hour
    one_hour_ago = (datetime.now() - timedelta(hours=1)).isoformat()
    
    critical_errors = db.select("error_logs", {
        "level": {"$in": ["CRITICAL", "ERROR"]},
        "timestamp": {"$gte": one_hour_ago}
    })
    
    if len(critical_errors) > 10:
        return {
            "alert": True,
            "message": f"High number of critical errors: {len(critical_errors)}",
            "errors": critical_errors[:5]  # Return first 5 for review
        }
    
    return {"alert": False}

# Usage
sample_logs = [
    "2024-01-01 10:30:45 [INFO] User alice@example.com accessed /dashboard",
    "2024-01-01 10:31:12 [ERROR] Database connection failed",
    "2024-01-01 10:32:00 [INFO] User bob@example.com accessed /profile",
    "2024-01-01 10:35:23 [ERROR] Network timeout on external API call",
]

for log_line in sample_logs:
    if "ERROR" in log_line or "CRITICAL" in log_line:
        parse_and_store_log_line(log_line, "error")
    else:
        parse_and_store_log_line(log_line, "access")

# Analysis
access_patterns = analyze_access_patterns()
print("Access patterns:", access_patterns)

error_summary = get_error_summary()
print("Error summary:", error_summary)

alerts = check_for_critical_errors()
if alerts["alert"]:
    print(f"ALERT: {alerts['message']}")
```

### Example 4: Content Management System

```python
from cafedb import CafeDB
from datetime import datetime
import hashlib

db = CafeDB("cms.json", verbose=True)
db.create_table("articles")
db.create_table("authors")
db.create_table("categories")

# Author management
def create_author(name, email, bio):
    # Check if author exists
    existing = db.select("authors", {"email": email})
    if existing:
        return existing[0]
    
    author = {
        "id": hashlib.md5(email.encode()).hexdigest()[:8],
        "name": name,
        "email": email,
        "bio": bio,
        "created_at": datetime.now().isoformat(),
        "article_count": 0
    }
    
    db.insert("authors", author)
    return author

# Article management
def publish_article(title, content, author_email, category, tags):
    # Get author
    authors = db.select("authors", {"email": author_email})
    if not authors:
        raise ValueError("Author not found")
    
    author = authors[0]
    
    article = {
        "id": hashlib.md5(f"{title}{author_email}".encode()).hexdigest()[:12],
        "title": title,
        "content": content,
        "author_id": author["id"],
        "author_name": author["name"],
        "category": category,
        "tags": tags,
        "status": "published",
        "views": 0,
        "likes": 0,
        "published_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat()
    }
    
    db.insert("articles", article)
    
    # Update author article count
    db.update("authors", 
        {"email": author_email},
        lambda a: {**a, "article_count": a.get("article_count", 0) + 1}
    )
    
    return article

# Content discovery
def search_articles(keyword=None, category=None, author=None, tag=None, status="published"):
    filters = {"status": status}
    
    if keyword:
        # Search in title and content (limitation: can only search one field)
        # Workaround: search title OR create a full-text search field
        filters["title"] = {"$contains": keyword}
    
    if category:
        filters["category"] = category
    
    if author:
        filters["author_name"] = {"$contains": author}
    
    if tag:
        # Assuming tags is a comma-separated string or list stored as string
        filters["tags"] = {"$contains": tag}
    
    return db.select("articles", filters)

# Article interactions
def increment_views(article_id):
    articles = db.select("articles", {"id": article_id})
    if not articles:
        return False
    
    db.update("articles",
        {"id": article_id},
        lambda a: {**a, "views": a.get("views", 0) + 1}
    )
    return True

def like_article(article_id):
    articles = db.select("articles", {"id": article_id})
    if not articles:
        return False
    
    db.update("articles",
        {"id": article_id},
        lambda a: {**a, "likes": a.get("likes", 0) + 1}
    )
    return True

# Analytics
def get_content_stats():
    all_articles = db.select("articles", {"status": "published"})
    
    total_articles = len(all_articles)
    total_views = sum(article.get("views", 0) for article in all_articles)
    total_likes = sum(article.get("likes", 0) for article in all_articles)
    
    # Most popular articles
    popular = sorted(all_articles, key=lambda x: x.get("views", 0), reverse=True)[:5]
    
    # Category distribution
    categories = {}
    for article in all_articles:
        cat = article.get("category", "Uncategorized")
        categories[cat] = categories.get(cat, 0) + 1
    
    return {
        "total_articles": total_articles,
        "total_views": total_views,
        "total_likes": total_likes,
        "avg_views": total_views / total_articles if total_articles > 0 else 0,
        "popular_articles": [{"title": a["title"], "views": a["views"]} for a in popular],
        "categories": categories
    }

# Usage
author1 = create_author("Alice Johnson", "alice@example.com", "Tech writer and developer")
author2 = create_author("Bob Smith", "bob@example.com", "Data scientist and ML expert")

article1 = publish_article(
    "Introduction to Python",
    "Python is a versatile programming language...",
    "alice@example.com",
    "Programming",
    ["python", "tutorial", "beginner"]
)

article2 = publish_article(
    "Machine Learning Basics",
    "Machine learning is transforming how we...",
    "bob@example.com",
    "Data Science",
    ["machine-learning", "ai", "data-science"]
)

# Simulate interactions
increment_views(article1["id"])
increment_views(article1["id"])
like_article(article1["id"])

# Search and discover
python_articles = search_articles(keyword="Python")
ml_articles = search_articles(tag="machine-learning")

stats = get_content_stats()
print("Content stats:", stats)
```

### Example 5: Task Management System

```python
from cafedb import CafeDB
from datetime import datetime, timedelta
import uuid

db = CafeDB("tasks.json", verbose=True)
db.create_table("tasks")
db.create_table("projects")
db.create_table("comments")

# Project management
def create_project(name, description, owner):
    project = {
        "id": str(uuid.uuid4()),
        "name": name,
        "description": description,
        "owner": owner,
        "status": "active",
        "created_at": datetime.now().isoformat(),
        "task_count": 0,
        "completed_tasks": 0
    }
    
    db.insert("projects", project)
    return project

# Task management
def create_task(title, description, project_id, assignee, priority="medium", due_date=None):
    task = {
        "id": str(uuid.uuid4()),
        "title": title,
        "description": description,
        "project_id": project_id,
        "assignee": assignee,
        "priority": priority,
        "status": "todo",
        "due_date": due_date,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "completed_at": None
    }
    
    db.insert("tasks", task)
    
    # Update project task count
    db.update("projects",
        {"id": project_id},
        lambda p: {**p, "task_count": p.get("task_count", 0) + 1}
    )
    
    return task

# Task updates
def update_task_status(task_id, new_status):
    tasks = db.select("tasks", {"id": task_id})
    if not tasks:
        return False
    
    task = tasks[0]
    updates = {
        "status": new_status,
        "updated_at": datetime.now().isoformat()
    }
    
    if new_status == "completed":
        updates["completed_at"] = datetime.now().isoformat()
        
        # Update project completed count
        db.update("projects",
            {"id": task["project_id"]},
            lambda p: {**p, "completed_tasks": p.get("completed_tasks", 0) + 1}
        )
    
    db.update("tasks", {"id": task_id}, updates)
    return True

# Query functions
def get_my_tasks(assignee, status=None):
    filters = {"assignee": assignee}
    if status:
        filters["status"] = status
    return db.select("tasks", filters)

def get_overdue_tasks():
    today = datetime.now().date().isoformat()
    
    # Get all non-completed tasks
    tasks = db.select("tasks", {
        "status": {"$nin": ["completed", "cancelled"]}
    })
    
    # Filter overdue (due_date exists and is in the past)
    overdue = []
    for task in tasks:
        due_date = task.get("due_date")
        if due_date and due_date < today:
            overdue.append(task)
    
    return overdue

def get_high_priority_tasks():
    return db.select("tasks", {
        "priority": "high",
        "status": {"$nin": ["completed", "cancelled"]}
    })

def get_project_progress(project_id):
    projects = db.select("projects", {"id": project_id})
    if not projects:
        return None
    
    project = projects[0]
    tasks = db.select("tasks", {"project_id": project_id})
    
    total_tasks = len(tasks)
    completed_tasks = len([t for t in tasks if t["status"] == "completed"])
    in_progress = len([t for t in tasks if t["status"] == "in_progress"])
    todo = len([t for t in tasks if t["status"] == "todo"])
    
    progress_percent = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0
    
    return {
        "project": project["name"],
        "total_tasks": total_tasks,
        "completed": completed_tasks,
        "in_progress": in_progress,
        "todo": todo,
        "progress_percent": round(progress_percent, 1)
    }

# Team analytics
def get_team_workload():
    tasks = db.select("tasks", {
        "status": {"$nin": ["completed", "cancelled"]}
    })
    
    workload = {}
    for task in tasks:
        assignee = task.get("assignee", "Unassigned")
        if assignee not in workload:
            workload[assignee] = {
                "total": 0,
                "high_priority": 0,
                "overdue": 0
            }
        
        workload[assignee]["total"] += 1
        
        if task.get("priority") == "high":
            workload[assignee]["high_priority"] += 1
        
        due_date = task.get("due_date")
        if due_date and due_date < datetime.now().date().isoformat():
            workload[assignee]["overdue"] += 1
    
    return workload

# Usage
project = create_project(
    "Website Redesign",
    "Complete redesign of company website",
    "alice@example.com"
)

task1 = create_task(
    "Design homepage mockup",
    "Create initial design concepts",
    project["id"],
    "bob@example.com",
    priority="high",
    due_date=(datetime.now() + timedelta(days=3)).date().isoformat()
)

task2 = create_task(
    "Review color scheme",
    "Finalize brand colors",
    project["id"],
    "alice@example.com",
    priority="medium",
    due_date=(datetime.now() + timedelta(days=5)).date().isoformat()
)

# Update task status
update_task_status(task1["id"], "in_progress")

# Get task lists
my_tasks = get_my_tasks("alice@example.com")
high_priority = get_high_priority_tasks()
overdue = get_overdue_tasks()

# Project tracking
progress = get_project_progress(project["id"])
print("Project progress:", progress)

workload = get_team_workload()
print("Team workload:", workload)
```

## 🔧 API Reference

### Database Class

#### `__init__(db_path: str, verbose: bool = False)`
Initialize or open a database.

**Parameters:**
- `db_path` (str): Path to JSON database file
- `verbose` (bool): Enable detailed logging

**Example:**
```python
db = CafeDB("myapp.json", verbose=True)
```

---

#### `create_table(table_name: str)`
Create a new table.

**Parameters:**
- `table_name` (str): Name of the table to create

**Raises:**
- `ValueError`: If table already exists

**Example:**
```python
db.create_table("users")
```

---

#### `drop_table(table_name: str)`
Delete a table and all its data.

**Parameters:**
- `table_name` (str): Name of the table to drop

**Raises:**
- `ValueError`: If table doesn't exist

**Example:**
```python
db.drop_table("old_table")
```

---

#### `insert(table_name: str, row: dict)`
Insert a new record into a table.

**Parameters:**
- `table_name` (str): Name of the table
- `row` (dict): Data to insert

**Raises:**
- `ValueError`: If table doesn't exist

**Example:**
```python
db.insert("users", {"name": "Alice", "age": 30})
```

---

#### `select(table_name: str, filters: Union[Dict, Callable] = None) -> List[dict]`
Query records from a table.

**Parameters:**
- `table_name` (str): Name of the table
- `filters` (dict or callable): Filter conditions or function

**Returns:**
- List of matching records

**Raises:**
- `ValueError`: If table doesn't exist or invalid operator

**Examples:**
```python
# All records
all_users = db.select("users")

# With filters
adults = db.select("users", {"age": {"$gte": 18}})

# With custom function
young = db.select("users", lambda u: u.get("age", 0) < 25)
```

---

#### `update(table_name: str, filters: Union[Dict, Callable], updater: Union[Dict, Callable]) -> int`
Update records matching filters.

**Parameters:**
- `table_name` (str): Name of the table
- `filters` (dict or callable): Filter conditions
- `updater` (dict or callable): Updates to apply

**Returns:**
- Number of records updated

**Examples:**
```python
# Update with dict
count = db.update("users", {"city": "Paris"}, {"timezone": "CET"})

# Update with function
count = db.update("users", 
    {"age": {"$gte": 65}},
    lambda u: {**u, "category": "senior"}
)
```

---

#### `delete(table_name: str, filters: Union[Dict, Callable]) -> int`
Delete records matching filters.

**Parameters:**
- `table_name` (str): Name of the table
- `filters` (dict or callable): Filter conditions

**Returns:**
- Number of records deleted

**Example:**
```python
count = db.delete("users", {"active": False})
```

---

#### `count(table_name: str, filters: Union[Dict, Callable] = None) -> int`
Count records matching filters.

**Parameters:**
- `table_name` (str): Name of the table
- `filters` (dict or callable, optional): Filter conditions

**Returns:**
- Number of matching records

**Example:**
```python
total = db.count("users")
active = db.count("users", {"active": True})
```

---

#### `list_tables() -> List[str]`
Get list of all tables in database.

**Returns:**
- List of table names

**Example:**
```python
tables = db.list_tables()
print(f"Tables: {', '.join(tables)}")
```

---

#### `exists_table(table_name: str) -> bool`
Check if a table exists.

**Parameters:**
- `table_name` (str): Name of the table

**Returns:**
- True if table exists, False otherwise

**Example:**
```python
if db.exists_table("users"):
    print("Users table exists")
```

---

#### `stats(table_name: str) -> dict`
Get detailed statistics about a table.

**Parameters:**
- `table_name` (str): Name of the table

**Returns:**
- Dictionary containing table statistics

**Example:**
```python
stats = db.stats("users")
print(f"Total rows: {stats['total_rows']}")
print(f"Fields: {stats['fields']}")
```

## 🔄 Migration Guide

### From Legacy Function-Based Queries

If you're using the old callable filter style:

```python
# Old style (still supported)
results = db.select("users", lambda u: u.get("age", 0) > 25)

# New style (recommended)
results = db.select("users", {"age": {"$gt": 25}})
```

### From SQL Databases

Common SQL patterns translated to CafeDB:

```sql
-- SQL: SELECT * FROM users WHERE age >= 18
```
```python
# CafeDB
db.select("users", {"age": {"$gte": 18}})
```

```sql
-- SQL: SELECT * FROM users WHERE city IN ('Paris', 'London')
```
```python
# CafeDB
db.select("users", {"city": {"$in": ["Paris", "London"]}})
```

```sql
-- SQL: SELECT * FROM users WHERE name LIKE 'A%'
```
```python
# CafeDB
db.select("users", {"name": "A*"})
```

```sql
-- SQL: SELECT * FROM users WHERE age BETWEEN 18 AND 65
```
```python
# CafeDB
db.select("users", {"age": {"$between": [18, 65]}})
```

## 🤝 Contributing

Contributions are welcome! Here's how you can help:

1. **Report Bugs**: Open an issue with detailed reproduction steps
2. **Suggest Features**: Describe your use case and proposed solution
3. **Submit Pull Requests**: Follow the coding style and add tests
4. **Improve Documentation**: Fix typos, add examples, clarify instructions

## 📝 License

MIT License - see LICENSE file for details.

## 🙋 FAQ

**Q: Can CafeDB handle millions of records?**  
A: CafeDB is optimized for small to medium datasets (< 100K records). For larger datasets, consider a traditional database.

**Q: Is CafeDB thread-safe?**  
A: The current implementation uses file-level locking, which provides basic thread safety. For high-concurrency scenarios, use a proper database.

**Q: Can I use CafeDB in production?**  
A: CafeDB is great for prototypes, small applications, and internal tools. For mission-critical production systems with high load, use PostgreSQL, MongoDB, etc.

**Q: How do I backup my database?**  
A: Simply copy the JSON file: `cp mydb.json mydb.backup.json`

**Q: Can I query nested fields?**  
A: Currently, queries work on top-level fields only. For nested data, flatten it or use custom logic.

**Q: What about JOIN operations?**  
A: CafeDB doesn't support JOINs. You'll need to query multiple tables and combine results in your application code.

**Q: How do I implement full-text search?**  
A: Use the `$contains` operator for simple text search, or `$regex` for pattern matching. For advanced full-text search, integrate with a dedicated search engine.


---

**Made with ☕ by the CafeDB Team**

*Simple databases for simple needs*
