"""
CafeDB Command Line Interface

Run the CafeDB demo:
    python -m cafedb
"""

from cafedb import CafeDB
from datetime import datetime


def run_demo():
    """Run CafeDB demonstration with sample data and queries"""
    
    print("=" * 60)
    print("CafeDB - Lightweight JSON Database Demo")
    print("=" * 60)
    print()
    
    # Initialize database
    print("Initializing database...")
    db = CafeDB("demo.json", verbose=True)
    print()
    
    # Create table
    print("Creating 'users' table...")
    if not db.exists_table("users"):
        db.create_table("users")
    else:
        print("Table already exists, clearing it...")
        db.clear_table("users")
    print()
    
    # Insert sample data
    print("Inserting sample data...")
    sample_users = [
        {"name": "Alice Johnson", "age": 28, "city": "Paris", "email": "alice@gmail.com", "score": 85},
        {"name": "Bob Smith", "age": 34, "city": "London", "email": "bob@yahoo.com", "score": 72},
        {"name": "Anna Miller", "age": 22, "city": "Berlin", "email": "anna@gmail.com", "score": 91},
        {"name": "Charlie Brown", "age": 45, "city": "Paris", "email": "charlie@hotmail.com", "score": 68},
        {"name": "Amy Wilson", "age": 31, "city": "London", "email": "amy@gmail.com", "score": 88},
    ]
    
    db.insert_many("users", sample_users)
    print(f"Inserted {len(sample_users)} users")
    print()
    
    # Demonstrate various queries
    print("=" * 60)
    print("Query Examples")
    print("=" * 60)
    print()
    
    # 1. Wildcard matching
    print("1. Users with names starting with 'A':")
    results = db.select("users", {"name": "A*"})
    for user in results:
        print(f"   - {user['name']} ({user['age']} years old)")
    print()
    
    # 2. String operations
    print("2. Gmail users:")
    results = db.select("users", {"email": "*@gmail.com"})
    for user in results:
        print(f"   - {user['name']} - {user['email']}")
    print()
    
    # 3. Range queries
    print("3. Users aged 25-35:")
    results = db.select("users", {"age": {"$between": [25, 35]}})
    for user in results:
        print(f"   - {user['name']} - {user['age']} years old")
    print()
    
    # 4. OR queries
    print("4. Users from Paris OR with high scores (>=85):")
    results = db.select("users", {
        "$or": [
            {"city": "Paris"},
            {"score": {"$gte": 85}}
        ]
    })
    for user in results:
        print(f"   - {user['name']} - {user['city']} (Score: {user['score']})")
    print()
    
    # 5. Sorting and pagination
    print("5. Top 3 users by score:")
    results = db.select("users", order_by="score", reverse=True, limit=3)
    for i, user in enumerate(results, 1):
        print(f"   {i}. {user['name']} - Score: {user['score']}")
    print()
    
    # 6. Complex query
    print("6. Young Gmail users in major cities:")
    results = db.select("users", {
        "age": {"$lt": 30},
        "email": "*@gmail.com",
        "city": {"$in": ["Paris", "London", "Berlin"]}
    })
    for user in results:
        print(f"   - {user['name']} ({user['age']}) in {user['city']}")
    print()
    
    # Statistics
    print("=" * 60)
    print("Database Statistics")
    print("=" * 60)
    print()
    
    info = db.info()
    print(f"Database path: {info['path']}")
    print(f"Tables: {info['table_count']}")
    print(f"Total rows: {info['total_rows']}")
    print(f"Created: {info['created']}")
    print()
    
    stats = db.stats("users")
    print(f"Table 'users' statistics:")
    print(f"  Total rows: {stats['total_rows']}")
    print(f"  Size: {stats['size_kb']} KB")
    print(f"\n  Field Statistics:")
    
    for field, info in stats['fields'].items():
        if not field.startswith('_'):
            print(f"    {field}:")
            print(f"      - Unique values: {info['unique_count']}")
            print(f"      - Present in: {info['present_percentage']}% of rows")
            if 'min' in info:
                print(f"      - Range: {info['min']} - {info['max']} (avg: {info['avg']})")
    print()
    
    # Transaction example
    print("=" * 60)
    print("Transaction Demo")
    print("=" * 60)
    print()
    
    try:
        with db.transaction():
            print("Starting transaction...")
            db.update("users", {"age": {"$gte": 40}}, {"category": "senior"})
            db.update("users", {"score": {"$gte": 85}}, {"level": "expert"})
            print("Transaction completed successfully!")
    except Exception as e:
        print(f"Transaction failed: {e}")
    print()
    
    print("=" * 60)
    print("Demo completed! Check 'demo.json' to see the data.")
    print("=" * 60)


if __name__ == "__main__":
    try:
        run_demo()
    except KeyboardInterrupt:
        print("\n\nDemo interrupted by user.")
    except Exception as e:
        print(f"\nError running demo: {e}")
        raise
