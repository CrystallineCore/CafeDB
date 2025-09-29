from cafedb import CafeDB
from datetime import datetime

# Initialize database
db = CafeDB("user_management.cdb", verbose=True)
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
