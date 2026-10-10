# Practical 1 - Task 1: Parsing Multi-Format Files & Anomaly Detection

**Author:** Wali Momin

---

## 1. Aim
To parse raw text, CSV, HTML, XML, and JSON files, extract relevant structured data, and identify data anomalies and missing values using Python standard library modules.

---

## 2. Brief Theory
Data engineering pipelines must regularly ingest semi-structured and unstructured data from heterogeneous origins:
- **Plain Text:** Unstructured textual data parsed using regex or line tokenization.
- **CSV (Comma-Separated Values):** Tabular data parsed using Python's `csv` module (`csv.reader` or `csv.DictReader`).
- **HTML (HyperText Markup Language):** Web data parsed using `html.parser.HTMLParser` by intercepting tags and character data.
- **XML (eXtensible Markup Language):** Hierarchical document trees traversed using `xml.etree.ElementTree`.
- **JSON (JavaScript Object Notation):** Lightweight key-value object trees parsed into Python native dictionaries using `json.loads` or `json.load`.

Anomaly detection identifies corrupted data types, non-numeric values, out-of-range records, or missing mandatory fields prior to downstream consumption.

---

## 3. Code
```python
"""
Practical 1 - Exercise 1
Parse text, CSV, HTML, XML and JSON files, extract relevant data,
and check for anomalies and missing values.
Standard library only. Runs locally and on Google Colab.
"""
import csv
import json
import re
import xml.etree.ElementTree as ET
from html.parser import HTMLParser


STUDENT_NAME = "Wali Momin"

# sample data
SAMPLES = {
    "sample.txt": (
        "Name: Asha, Age: 21, City: Pune\n"
        "Name: Ravi, Age: , City: Delhi\n"
        "Name: Meena, Age: 250, City: Chennai\n"
        "Name: Asha, Age: 21, City: Pune\n"
        "Bad line without fields\n"
    ),
    "sample.csv": (
        "id,name,age,salary\n"
        "1,Asha,21,50000\n"
        "2,Ravi,,42000\n"
        "3,Meena,34,\n"
        "4,John,-5,61000\n"
        "4,John,-5,61000\n"
    ),
    "sample.html": (
        "<html><body><h1>Products</h1>"
        "<table><tr><th>Item</th><th>Price</th></tr>"
        "<tr><td>Pen</td><td>10</td></tr>"
        "<tr><td>Book</td><td></td></tr>"
        "<tr><td>Bag</td><td>abc</td></tr></table>"
        "<a href='https://example.com'>Home</a>"
        "<a href='https://python.org'>Python</a></body></html>"
    ),
    "sample.xml": (
        "<students>"
        "<student id='1'><name>Asha</name><marks>88</marks></student>"
        "<student id='2'><name>Ravi</name></student>"
        "<student id='3'><name>Meena</name><marks>140</marks></student>"
        "</students>"
    ),
    "sample.json": json.dumps(
        [
            {"id": 1, "name": "Asha", "email": "asha@mail.com", "age": 21},
            {"id": 2, "name": "Ravi", "email": None, "age": 30},
            {"id": 3, "name": "Meena", "email": "meena@mail", "age": -2},
            {"id": 3, "name": "Meena", "email": "meena@mail", "age": -2},
        ],
        indent=2,
    ),
}


def create_samples():
    for fname, content in SAMPLES.items():
        with open(fname, "w", encoding="utf-8") as f:
            f.write(content)


def report(title, issues):
    print(f"\n--- Anomaly report: {title} ---")
    if issues:
        for i in issues:
            print("  [!]", i)
        print(f"  Total anomalies: {len(issues)}")
    else:
        print("  No anomalies found.")


def to_number(value):
    """Return value as float, or None if it is not numeric."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# EXERCISE 1
# TEXT
def parse_text(path):
    print("\n===== TEXT FILE =====")
    pattern = re.compile(r"Name:\s*(.*?),\s*Age:\s*(.*?),\s*City:\s*(.*)")
    records, issues, seen = [], [], set()
    with open(path, encoding="utf-8") as f:
        for n, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            m = pattern.match(line)
            if not m:
                issues.append(f"Line {n}: malformed -> '{line}'")
                continue
            name, age, city = (g.strip() for g in m.groups())
            if not name or not city:
                issues.append(f"Line {n}: missing name or city")
            if not age:
                issues.append(f"Line {n}: missing age for {name}")
            elif not age.isdigit() or not (0 < int(age) < 120):
                issues.append(f"Line {n}: invalid age '{age}'")
            if (name, age, city) in seen:
                issues.append(f"Line {n}: duplicate record")
            seen.add((name, age, city))
            records.append({"name": name, "age": age, "city": city})
    print("Extracted records:")
    for r in records:
        print(" ", r)
    report("text", issues)


# CSV
def parse_csv(path):
    print("\n===== CSV FILE =====")
    issues, seen = [], set()
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print("Extracted rows:")
    for r in rows:
        print(" ", r)
    for n, r in enumerate(rows, 2):
        for col, val in r.items():
            if val is None or str(val).strip() == "":
                issues.append(f"Row {n}: missing value in '{col}'")
        for col in ("age", "salary"):
            val = r.get(col)
            if val and val.strip():
                num = to_number(val)
                if num is None:
                    issues.append(f"Row {n}: non-numeric {col} '{val}'")
                elif num < 0:
                    issues.append(f"Row {n}: negative {col} ({val})")
        key = tuple(r.values())
        if key in seen:
            issues.append(f"Row {n}: duplicate row")
        seen.add(key)
    report("CSV", issues)


# HTML
class PageParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows, self.row, self.cell = [], None, None
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == "tr":
            self.row = []
        elif tag in ("td", "th"):
            self.cell = ""
        elif tag == "a":
            self.links.extend(v for k, v in attrs if k == "href")

    def handle_data(self, data):
        if self.cell is not None:
            self.cell += data

    def handle_endtag(self, tag):
        if tag in ("td", "th") and self.row is not None:
            self.row.append(self.cell.strip())
            self.cell = None
        elif tag == "tr" and self.row is not None:
            self.rows.append(self.row)
            self.row = None


def parse_html(path):
    print("\n===== HTML FILE =====")
    with open(path, encoding="utf-8") as f:
        p = PageParser()
        p.feed(f.read())
    issues = []
    if not p.rows:
        report("HTML", ["No table rows found"])
        return
    header, *data = p.rows
    print("Header:", header)
    print("Data  :", data)
    print("Links :", p.links)
    for n, row in enumerate(data, 1):
        if len(row) != len(header):
            issues.append(f"Row {n}: has {len(row)} cells, expected {len(header)}")
            continue
        item, price = row[0], row[1]
        if not price:
            issues.append(f"Row {n} ({item}): missing price")
        elif to_number(price) is None:
            issues.append(f"Row {n} ({item}): non-numeric price '{price}'")
    report("HTML", issues)


# XML
def parse_xml(path):
    print("\n===== XML FILE =====")
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError as err:
        report("XML", [f"Malformed XML document: {err}"])
        return
    issues = []
    for s in root.findall("student"):
        sid = s.get("id")
        name = s.findtext("name")
        marks = s.findtext("marks")
        print({"id": sid, "name": name, "marks": marks})
        if not name:
            issues.append(f"Student {sid}: <name> missing")
        if marks is None or marks.strip() == "":
            issues.append(f"Student {sid} ({name}): <marks> missing")
        else:
            num = to_number(marks)
            if num is None:
                issues.append(f"Student {sid} ({name}): non-numeric marks '{marks}'")
            elif not (0 <= num <= 100):
                issues.append(f"Student {sid} ({name}): marks out of range ({marks})")
    report("XML", issues)


# JSON
def parse_json(path):
    print("\n===== JSON FILE =====")
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as err:
        report("JSON", [f"Malformed JSON document: {err}"])
        return
    issues, seen = [], set()
    email_re = re.compile(r"^[\w.+-]+@[\w-]+\.[\w.]+$")
    for n, rec in enumerate(data, 1):
        print(rec)
        for k, v in rec.items():
            if v is None or v == "":
                issues.append(f"Record {n}: missing/null value for '{k}'")
        if rec.get("email") and not email_re.match(rec["email"]):
            issues.append(f"Record {n}: invalid email '{rec['email']}'")
        age = rec.get("age")
        if age is not None:
            if not isinstance(age, (int, float)) or isinstance(age, bool):
                issues.append(f"Record {n}: age is not a number ({age!r})")
            elif age < 0:
                issues.append(f"Record {n}: negative age ({age})")
        key = json.dumps(rec, sort_keys=True)
        if key in seen:
            issues.append(f"Record {n}: duplicate record")
        seen.add(key)
    report("JSON", issues)


def exercise1():
    print("\n" + "#" * 60 + "\n# EXERCISE 1: Parsing text, CSV, HTML, XML, JSON\n" + "#" * 60)
    create_samples()
    parse_text("sample.txt")
    parse_csv("sample.csv")
    parse_html("sample.html")
    parse_xml("sample.xml")
    parse_json("sample.json")


if __name__ == "__main__":
    exercise1()
    print("\n" + "=" * 40)
    print("Submitted by:", STUDENT_NAME)
    print("=" * 40)
```

---

## 4. Output
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
  Total anomalies: 4

===== CSV FILE =====
Extracted rows:
  {'id': '1', 'name': 'Asha', 'age': '21', 'salary': '50000'}
  {'id': '2', 'name': 'Ravi', 'age': '', 'salary': '42000'}
  {'id': '3', 'name': 'Meena', 'age': '34', 'salary': ''}
  {'id': '4', 'name': 'John', 'age': '-5', 'salary': '61000'}
  {'id': '4', 'name': 'John', 'age': '-5', 'salary': '61000'}

--- Anomaly report: CSV ---
  [!] Row 3: missing value in 'age'
  [!] Row 4: missing value in 'salary'
  [!] Row 5: negative age (-5)
  [!] Row 6: negative age (-5)
  [!] Row 6: duplicate row
  Total anomalies: 5

===== HTML FILE =====
Header: ['Item', 'Price']
Data  : [['Pen', '10'], ['Book', ''], ['Bag', 'abc']]
Links : ['https://example.com', 'https://python.org']

--- Anomaly report: HTML ---
  [!] Row 2 (Book): missing price
  [!] Row 3 (Bag): non-numeric price 'abc'
  Total anomalies: 2

===== XML FILE =====
{'id': '1', 'name': 'Asha', 'marks': '88'}
{'id': '2', 'name': 'Ravi', 'marks': None}
{'id': '3', 'name': 'Meena', 'marks': '140'}

--- Anomaly report: XML ---
  [!] Student 2 (Ravi): <marks> missing
  [!] Student 3 (Meena): marks out of range (140)
  Total anomalies: 2

===== JSON FILE =====
{'id': 1, 'name': 'Asha', 'email': 'asha@mail.com', 'age': 21}
{'id': 2, 'name': 'Ravi', 'email': None, 'age': 30}
{'id': 3, 'name': 'Meena', 'email': 'meena@mail', 'age': -2}
{'id': 3, 'name': 'Meena', 'email': 'meena@mail', 'age': -2}

--- Anomaly report: JSON ---
  [!] Record 2: missing/null value for 'email'
  [!] Record 3: invalid email 'meena@mail'
  [!] Record 3: negative age (-2)
  [!] Record 4: invalid email 'meena@mail'
  [!] Record 4: negative age (-2)
  [!] Record 4: duplicate record
  Total anomalies: 6

========================================
Submitted by: Wali Momin
========================================
```

---

## 5. Conclusion
Successfully implemented multi-format file parsing using standard Python libraries without external dependencies. Demonstrated effective anomaly detection and data cleaning techniques for corrupted and incomplete records across Text, CSV, HTML, XML, and JSON data formats.
