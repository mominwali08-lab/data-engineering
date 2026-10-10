# Practical 1 - Task 4: Relational Database Schema & CRUD Operations

**Author:** Wali Momin

---

## 1. Aim
To design a relational database schema, populate it with records, enforce relational constraints, and execute SQL CRUD operations using SQLite.

---

## 2. Brief Theory
Relational Database Management Systems (RDBMS) organize data into normalized tables with strict schema enforcement:
- **Schema Design & Integrity:**
  - `PRIMARY KEY`: Uniquely identifies each record.
  - `FOREIGN KEY`: Establishes parent-child relationships and maintains referential integrity.
  - `ON DELETE CASCADE`: Automatically propagates deletions from parents to related child records.
- **CRUD Operations:**
  - **Create:** `CREATE TABLE`, `INSERT INTO`
  - **Read:** `SELECT ... FROM ... WHERE ... JOIN ... GROUP BY`
  - **Update:** `UPDATE ... SET ... WHERE`
  - **Delete:** `DELETE FROM ... WHERE`
- **Joins & Aggregations:** Multi-table joins (`INNER JOIN`) correlate entities, while `GROUP BY` and aggregate functions (`COUNT`, `SUM`, `AVG`) compute summary statistics.

---

## 3. Code
```python
"""
Practical 1 - Exercise 4
Design a relational database, populate it and perform CRUD operations in SQL.
Standard library only. Runs locally and on Google Colab.
"""
import os
import sqlite3


STUDENT_NAME = "Wali Momin"

# EXERCISE 4
def run(cur, sql, params=()):
    print("SQL>", " ".join(sql.split()))
    cur.execute(sql, params)


def show(cur, title):
    print(f"-- {title}")
    for row in cur.fetchall():
        print("  ", row)


def exercise4():
    print("\n" + "#" * 60 + "\n# EXERCISE 4: Relational database + CRUD (SQLite)\n" + "#" * 60)
    if os.path.exists("college.db"):
        os.remove("college.db")
    conn = sqlite3.connect("college.db")
    conn.execute("PRAGMA foreign_keys = ON")
    cur = conn.cursor()

    # CREATE (schema)
    print("\n=== CREATE: tables ===")
    cur.executescript("""
        CREATE TABLE departments (
            dept_id   INTEGER PRIMARY KEY,
            dept_name TEXT NOT NULL UNIQUE
        );
        CREATE TABLE students (
            student_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name       TEXT NOT NULL,
            email      TEXT UNIQUE,
            dept_id    INTEGER NOT NULL,
            FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
        );
        CREATE TABLE courses (
            course_id INTEGER PRIMARY KEY,
            title     TEXT NOT NULL,
            credits   INTEGER CHECK (credits > 0)
        );
        CREATE TABLE enrollments (
            student_id INTEGER,
            course_id  INTEGER,
            grade      TEXT,
            PRIMARY KEY (student_id, course_id),
            FOREIGN KEY (student_id) REFERENCES students(student_id) ON DELETE CASCADE,
            FOREIGN KEY (course_id)  REFERENCES courses(course_id)
        );
    """)
    print("Tables created: departments, students, courses, enrollments")
    print("\nDesign (relationships):")
    print("  departments 1 --- * students   (students.dept_id -> departments.dept_id)")
    print("  students    * --- * courses    via enrollments (student_id, course_id)")
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    print("Tables in database:", [r[0] for r in cur.fetchall()])

    # CREATE (insert rows)
    print("\n=== CREATE: insert rows ===")
    cur.executemany("INSERT INTO departments VALUES (?, ?)",
                    [(1, "Computer Science"), (2, "Mathematics"), (3, "Physics")])
    cur.executemany("INSERT INTO students (name, email, dept_id) VALUES (?, ?, ?)",
                    [("Asha", "asha@mail.com", 1), ("Ravi", "ravi@mail.com", 2),
                     ("Meena", "meena@mail.com", 1), ("John", "john@mail.com", 3)])
    cur.executemany("INSERT INTO courses VALUES (?, ?, ?)",
                    [(101, "Databases", 4), (102, "Statistics", 3), (103, "Python", 3)])
    cur.executemany("INSERT INTO enrollments VALUES (?, ?, ?)",
                    [(1, 101, "A"), (1, 103, "B"), (2, 102, "A"),
                     (3, 101, "B"), (3, 103, "A"), (4, 102, "C")])
    conn.commit()
    print("Sample data inserted.")

    # READ
    print("\n=== READ ===")
    run(cur, "SELECT * FROM students")
    show(cur, "All students")
    run(cur, "SELECT name FROM students WHERE dept_id = ? ORDER BY name", (1,))
    show(cur, "Students in dept 1")
    run(cur, """SELECT s.name, d.dept_name, c.title, e.grade
                FROM enrollments e
                JOIN students s    ON s.student_id = e.student_id
                JOIN courses c     ON c.course_id  = e.course_id
                JOIN departments d ON d.dept_id    = s.dept_id
                ORDER BY s.name""")
    show(cur, "Join: student / department / course / grade")
    run(cur, """SELECT d.dept_name, COUNT(*) AS total
                FROM students s JOIN departments d ON s.dept_id = d.dept_id
                GROUP BY d.dept_name""")
    show(cur, "Students per department (GROUP BY)")

    # UPDATE
    print("\n=== UPDATE ===")
    run(cur, "UPDATE students SET email = ? WHERE name = ?", ("asha.new@mail.com", "Asha"))
    run(cur, "UPDATE enrollments SET grade = 'A' WHERE student_id = 4 AND course_id = 102")
    conn.commit()
    run(cur, "SELECT student_id, name, email FROM students WHERE name = 'Asha'")
    show(cur, "After update")

    # DELETE
    print("\n=== DELETE ===")
    run(cur, "SELECT COUNT(*) FROM students")
    show(cur, "Student count before delete")
    run(cur, "DELETE FROM students WHERE name = ?", ("John",))
    conn.commit()
    print("Rows deleted:", cur.rowcount, "(his enrollments removed by ON DELETE CASCADE)")
    run(cur, "SELECT COUNT(*) FROM students")
    show(cur, "Student count after delete")
    run(cur, "SELECT * FROM enrollments")
    show(cur, "Enrollments after delete")

    # constraint demo
    print("\n=== Integrity check ===")
    try:
        cur.execute("INSERT INTO students (name, email, dept_id) VALUES ('X','x@m.com', 99)")
    except sqlite3.IntegrityError as err:
        print("Rejected invalid department ->", err)
    conn.close()


if __name__ == "__main__":
    exercise4()
    print("\n" + "=" * 40)
    print("Submitted by:", STUDENT_NAME)
    print("=" * 40)
```

---

## 4. Output
```text
############################################################
# EXERCISE 4: Relational database + CRUD (SQLite)
############################################################

=== CREATE: tables ===
Tables created: departments, students, courses, enrollments

Design (relationships):
  departments 1 --- * students   (students.dept_id -> departments.dept_id)
  students    * --- * courses    via enrollments (student_id, course_id)
Tables in database: ['departments', 'students', 'courses', 'enrollments']

=== CREATE: insert rows ===
Sample data inserted.

=== READ ===
SQL> SELECT * FROM students
-- All students
   (1, 'Asha', 'asha@mail.com', 1)
   (2, 'Ravi', 'ravi@mail.com', 2)
   (3, 'Meena', 'meena@mail.com', 1)
   (4, 'John', 'john@mail.com', 3)
SQL> SELECT name FROM students WHERE dept_id = ? ORDER BY name
-- Students in dept 1
   ('Asha',)
   ('Meena',)
SQL> SELECT s.name, d.dept_name, c.title, e.grade FROM enrollments e JOIN students s ON s.student_id = e.student_id JOIN courses c ON c.course_id = e.course_id JOIN departments d ON d.dept_id = s.dept_id ORDER BY s.name
-- Join: student / department / course / grade
   ('Asha', 'Computer Science', 'Databases', 'A')
   ('Asha', 'Computer Science', 'Python', 'B')
   ('John', 'Physics', 'Statistics', 'C')
   ('Meena', 'Computer Science', 'Databases', 'B')
   ('Meena', 'Computer Science', 'Python', 'A')
   ('Ravi', 'Mathematics', 'Statistics', 'A')
SQL> SELECT d.dept_name, COUNT(*) AS total FROM students s JOIN departments d ON s.dept_id = d.dept_id GROUP BY d.dept_name
-- Students per department (GROUP BY)
   ('Computer Science', 2)
   ('Mathematics', 1)
   ('Physics', 1)

=== UPDATE ===
SQL> UPDATE students SET email = ? WHERE name = ?
SQL> UPDATE enrollments SET grade = 'A' WHERE student_id = 4 AND course_id = 102
SQL> SELECT student_id, name, email FROM students WHERE name = 'Asha'
-- After update
   (1, 'Asha', 'asha.new@mail.com')

=== DELETE ===
SQL> SELECT COUNT(*) FROM students
-- Student count before delete
   (4,)
SQL> DELETE FROM students WHERE name = ?
Rows deleted: 1 (his enrollments removed by ON DELETE CASCADE)
SQL> SELECT COUNT(*) FROM students
-- Student count after delete
   (3,)
SQL> SELECT * FROM enrollments
-- Enrollments after delete
   (1, 101, 'A')
   (1, 103, 'B')
   (2, 102, 'A')
   (3, 101, 'B')
   (3, 103, 'A')

=== Integrity check ===
Rejected invalid department -> FOREIGN KEY constraint failed

========================================
Submitted by: Wali Momin
========================================
```

---

## 5. Conclusion
Successfully modeled a relational college database with departments, students, courses, and enrollments tables. Enforced referential integrity via foreign keys and cascade deletions, and performed complete SQL CRUD operations.
