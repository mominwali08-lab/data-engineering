# Practical 1: Data Ingestion, Binary File Processing, Regex Wrangling & Relational Databases

**Author:** Wali Momin

---

## 1. Aim
To master foundational data engineering techniques in Python:
1. Parsing heterogeneous multi-format files (Text, CSV, HTML, XML, JSON) and detecting anomalies.
2. Binary file serialization and deserialization using the `struct` module.
3. String manipulation, pattern matching, and validation using Regular Expressions (`re`).
4. Relational database schema design and executing full SQL CRUD operations using SQLite.

---

## 2. Brief Theory
Data engineering begins with robust data acquisition and preparation:
- **Semi-Structured Ingestion:** Different data providers serve data in varied formats. Data engineers must parse JSON, XML, HTML, and CSV while implementing anomaly filters for missing or corrupted entries.
- **Binary Data Encoding:** Storing data as raw binary using format strings (`<i10sf`) minimizes memory consumption and enables fast I/O.
- **Pattern Matching (Regex):** Text wrangling, log tokenization, and format standardization require regular expressions.
- **Relational Modeling (RDBMS):** Structured business data requires relational guarantees (ACID transactions, primary/foreign keys, referential integrity, and SQL CRUD queries).

---

## 3. Structure & Tasks
- [**Task 1: Multi-Format Parsing & Anomaly Detection**](./Task%201/README.md)
- [**Task 2: Reading & Writing Binary Files**](./Task%202/README.md)
- [**Task 3: Pattern Matching & String Processing with Regular Expressions**](./Task%203/README.md)
- [**Task 4: Relational Database Schema & CRUD Operations**](./Task%204/README.md)

---

## 4. Key Execution Output Summary

```text
############################################################
# EXERCISE 1: Parsing text, CSV, HTML, XML, JSON
############################################################

===== TEXT FILE =====
Extracted records:
  {'name': 'Asha', 'age': '21', 'city': 'Pune'}
  {'name': 'Ravi', 'age': '', 'city': 'Delhi'}
  {'name': 'Meena', 'age': '250', 'city': 'Chennai'}
  {'name': 'Asha', 'age': '21', 'city': 'Pune'}

--- Anomaly report: text ---
  [!] Line 2: missing age for Ravi
  [!] Line 3: invalid age '250'
  [!] Line 4: duplicate record
  [!] Line 5: malformed -> 'Bad line without fields'
  Total an
...
me, email FROM students WHERE name = 'Asha'
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
Practical 1 successfully established core data engineering capabilities across data ingestion, low-level binary data storage, text processing, and relational database management.
