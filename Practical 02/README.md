# Practical 2: Designing a Relational Database and Performing CRUD Operations using SQL

**Author:** Wali Momin

---

## 1. Aim
To design a relational database schema in SQLite, establish relationships using Primary and Foreign Keys, insert sample datasets, and execute comprehensive SQL CRUD (Create, Read, Update, Delete) operations with joins.

---

## 2. Brief Theory
Relational databases structure information into tables with distinct schemas:
- **Normalization & Relationships:** Data redundancy is reduced by splitting entities into distinct tables (`Students`, `Courses`, `Enrollments`) connected by `student_id` and `course_id`.
- **Primary & Foreign Keys:** Ensure entity identity and maintain referential integrity.
- **SQL CRUD Paradigm:**
  - **CREATE:** Creating schemas using `CREATE TABLE` with constraints (`PRIMARY KEY`, `AUTOINCREMENT`, `FOREIGN KEY`).
  - **READ:** Querying records via `SELECT`, filtering via `WHERE`, and combining tables via `JOIN` (`INNER JOIN`).
  - **UPDATE:** Modifying existing records via `UPDATE ... SET ... WHERE`.
  - **DELETE:** Removing specified records via `DELETE FROM ... WHERE`.
- **SQLite Engine:** Serverless, self-contained, transactional SQL database engine natively supported in Python via `sqlite3`.

---

## 3. Code
```python
# Practical 2: Designing a Relational Database and Performing CRUD using SQL
import sqlite3


STUDENT_NAME = "Wali Momin"

conn = sqlite3.connect("college.db")
cur = conn.cursor()

# CREATE: tables
cur.executescript("""
DROP TABLE IF EXISTS Students;
DROP TABLE IF EXISTS Courses;
DROP TABLE IF EXISTS Enrollments;

CREATE TABLE Students (
    student_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    age INTEGER
);
CREATE TABLE Courses (
    course_id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    credits INTEGER
);
CREATE TABLE Enrollments (
    enroll_id INTEGER PRIMARY KEY AUTOINCREMENT,
    student_id INTEGER REFERENCES Students(student_id),
    course_id INTEGER REFERENCES Courses(course_id),
    grade TEXT
);
""")

# CREATE: insert records
cur.executemany("INSERT INTO Students (name, email, age) VALUES (?, ?, ?)", [
    ("Asha Rao", "asha@example.com", 20),
    ("Ravi Kumar", "ravi@example.com", 21),
    ("Neha Shah", "neha@example.com", 22),
])
cur.executemany("INSERT INTO Courses (title, credits) VALUES (?, ?)", [
    ("Data Engineering", 4),
    ("Database Systems", 3),
])
cur.executemany("INSERT INTO Enrollments (student_id, course_id, grade) VALUES (?, ?, ?)", [
    (1, 1, "A"), (1, 2, "B+"), (2, 1, "A-"), (3, 2, "B"),
])
conn.commit()

# READ
print("All Students:")
for row in cur.execute("SELECT * FROM Students"):
    print(row)

print("\nStudents with their enrolled courses:")
cur.execute("""
    SELECT s.name, c.title, e.grade
    FROM Enrollments e
    JOIN Students s ON s.student_id = e.student_id
    JOIN Courses c ON c.course_id = e.course_id
""")
for row in cur.fetchall():
    print(row)

# UPDATE
cur.execute("UPDATE Students SET age = 23 WHERE name = 'Ravi Kumar'")
conn.commit()
print("\nAfter UPDATE:")
for row in cur.execute("SELECT * FROM Students WHERE name = 'Ravi Kumar'"):
    print(row)

# DELETE
cur.execute("DELETE FROM Enrollments WHERE grade = 'B'")
conn.commit()
print("\nAfter DELETE (grade='B' rows removed):")
for row in cur.execute("SELECT * FROM Enrollments"):
    print(row)

conn.close()

print("\n" + "=" * 40)
print("Submitted by:", STUDENT_NAME)
print("=" * 40)
```

---

## 4. Output
```text
All Students:
(1, 'Asha Rao', 'asha@example.com', 20)
(2, 'Ravi Kumar', 'ravi@example.com', 21)
(3, 'Neha Shah', 'neha@example.com', 22)

Students with their enrolled courses:
('Asha Rao', 'Data Engineering', 'A')
('Asha Rao', 'Database Systems', 'B+')
('Ravi Kumar', 'Data Engineering', 'A-')
('Neha Shah', 'Database Systems', 'B')

After UPDATE:
(2, 'Ravi Kumar', 'ravi@example.com', 23)

After DELETE (grade='B' rows removed):
(1, 1, 1, 'A')
(2, 1, 2, 'B+')
(3, 2, 1, 'A-')

========================================
Submitted by: Wali Momin
========================================
```

---

## 5. Conclusion
Successfully designed a normalized relational database in SQLite. Validated schema relationships with foreign key joins, executed transactional CRUD operations, and verified accurate row updates and record deletions.
