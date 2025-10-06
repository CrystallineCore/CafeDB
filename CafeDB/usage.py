from cafedb import CafeDB
from datetime import datetime
import uuid
import os

db = CafeDB("student_grades.cdb", verbose=False) 

def clear_screen():
    """Clears the terminal screen."""
    os.system('cls' if os.name == 'nt' else 'clear')

def print_header(title):
    """Prints a formatted header."""
    print("=" * 40)
    print(f"| {title.center(36)} |")
    print("=" * 40)

def press_enter_to_continue():
    """Pauses execution until the user presses Enter."""
    input("\nPress Enter to continue...")

def setup_database():
    """Creates the necessary tables if they don't exist."""
    try:
        db.create_table("students")
    except ValueError:
        pass 

    try:
        db.create_table("grades")
    except ValueError:
        pass 

def add_student_cli():
    """CLI function to add a new student."""
    print_header("Add New Student")
    name = input("Enter student's full name: ")
    email = input("Enter student's email: ")
    while db.select("students", {"email": email}):
        print("A student with this email already exists.")
        email = input("Enter a different email: ")

    try:
        enrollment_year = int(input("Enter enrollment year: "))
    except ValueError:
        print("Invalid year. Please enter a number.")
        return

    student = {
        "student_id": str(uuid.uuid4())[:8],
        "name": name,
        "email": email,
        "enrollment_year": enrollment_year,
        "created_at": datetime.now().isoformat()
    }
    db.insert("students", student)
    print(f"\n Student '{name}' added successfully with ID: {student['student_id']}")

def record_grade_cli():
    """CLI function to record a new grade."""
    print_header("Record a Grade")
    student_id = input("Enter the student ID to record a grade for: ")
    if not db.select("students", {"student_id": student_id}):
        print(f" Error: Student with ID '{student_id}' not found.")
        return

    subject = input("Enter the subject (e.g., Math, Science): ")
    try:
        score = float(input("Enter the score: "))
        if not 0 <= score <= 100:
            raise ValueError("Score must be between 0 and 100.")
    except ValueError as e:
        print(f" Invalid score. {e}")
        return

    grade = {
        "grade_id": str(uuid.uuid4())[:8],
        "student_id": student_id,
        "subject": subject,
        "score": score,
        "date": datetime.now().date().isoformat()
    }
    db.insert("grades", grade)
    print(f"\n Grade of {score} in {subject} recorded for student {student_id}.")

def view_student_grades_cli():
    """CLI function to view all grades for a specific student."""
    print_header("View Student Grades")
    student_id = input("Enter the student ID: ")
    student = db.select("students", {"student_id": student_id})
    if not student:
        print(f" Error: Student with ID '{student_id}' not found.")
        return

    grades = db.select("grades", {"student_id": student_id})
    print(f"\nGrades for {student[0]['name']} ({student_id}):")
    if not grades:
        print("No grades found for this student.")
    else:
        for grade in grades:
            print(f"  - Subject: {grade['subject']}, Score: {grade['score']}, Date: {grade['date']}")

def find_top_performers_cli():
    """CLI function to find top performers in a subject."""
    print_header("Find Top Performers")
    subject = input("Enter the subject to search in: ")
    try:
        min_score = float(input("Enter the minimum score to be considered a top performer (e.g., 90): "))
    except ValueError:
        print(" Invalid score.")
        return

    results = db.select("grades", {"subject": subject, "score": {"$gte": min_score}})
    print(f"\nTop Performers in {subject} (Score >= {min_score}):")
    if not results:
        print("No students met the criteria.")
    else:
        for grade in results:
            student = db.select("students", {"student_id": grade['student_id']})
            if student:
                print(f"  - Student: {student[0]['name']}, Score: {grade['score']}")

def list_all_students_cli():
    """Lists all students in the database."""
    print_header("All Students")
    students = db.select("students")
    if not students:
        print("No students in the database.")
    else:
        for student in students:
            print(f"  - Name: {student['name']}, Email: {student['email']}, ID: {student['student_id']}")

def delete_student_cli():
    """CLI function to delete a student."""
    print_header("Delete a Student")
    student_id = input("Enter the ID of the student to delete: ")
    student = db.select("students", {"student_id": student_id})
    if not student:
        print(f" Error: Student with ID '{student_id}' not found.")
        return

    confirm = input(f"Are you sure you want to delete {student[0]['name']} and all their grades? (y/n): ").lower()
    if confirm == 'y':
        db.delete("students", {"student_id": student_id})
        db.delete("grades", {"student_id": student_id})
        print(f" Student {student_id} has been deleted.")
    else:
        print("Deletion cancelled.")

def get_database_stats_cli():
    """Displays statistics for all tables."""
    print_header("Database Statistics")
    for table_name in db.list_tables():
        stats = db.stats(table_name)
        print(f"\n--- Table: {stats['table']} ---")
        print(f"  Total Rows: {stats['total_rows']}")
        if "fields" in stats and stats['fields']:
            print("  Fields:")
            for field, info in stats['fields'].items():
                print(f"    - {field}: Present: {info['present_count']}, Unique: {info['unique_count']}")
        else:
            print("  No fields or data in this table.")

def main_menu():
    """Displays the main menu and handles user input."""
    while True:
        clear_screen()
        print_header("Student Gradebook CLI")
        print("1. Add New Student")
        print("2. Record a Grade")
        print("3. View a Student's Grades")
        print("4. Find Top Performers in a Subject")
        print("5. List All Students")
        print("6. Delete a Student")
        print("7. View Database Statistics")
        print("0. Exit")
        print("-" * 40)
        choice = input("Enter your choice: ")

        if choice == '1':
            add_student_cli()
        elif choice == '2':
            record_grade_cli()
        elif choice == '3':
            view_student_grades_cli()
        elif choice == '4':
            find_top_performers_cli()
        elif choice == '5':
            list_all_students_cli()
        elif choice == '6':
            delete_student_cli()
        elif choice == '7':
            get_database_stats_cli()
        elif choice == '0':
            print("Goodbye!")
            break
        else:
            print(" Invalid choice. Please try again.")

        press_enter_to_continue()

if __name__ == "__main__":
    setup_database()
    main_menu()
