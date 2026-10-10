# Practical 7: End-to-End ETL Pipeline with Data Validation & Incremental Loading

**Author:** Wali Momin

---

## 1. Aim
To construct a complete ETL pipeline that extracts data from multiple CSV and JSON files, enforces rigorous data validation rules, isolates rejected records, performs transformations, and executes idempotent incremental loads into an SQLite relational database.

---

## 2. Brief Theory
Enterprise ETL pipelines require resilience against corrupted data and redundant loading:
- **Extract (E):** Ingestion of batch data from multiple disparate sources (monthly CSV files and nested JSON files).
- **Transform (T) & Quality Validation:**
  - Identifying missing customer names, invalid quantities, negative prices, and malformed dates.
  - Segregating valid records from rejected records with detailed audit reasons.
- **Load (L) & Incremental Ingestion:**
  - Full load vs Incremental load: Only newly arriving records or modified records are written.
  - Idempotency: Running the pipeline multiple times on the same input produces consistent state without duplicate entries (`INSERT ... WHERE key NOT IN (...)`).

---

## 3. Code
```python
import os
import json
import sqlite3
import pandas as pd

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)

STUDENT_NAME = "Wali Momin"
DB_FILE = "warehouse.db"
REQUIRED = ["order_id", "customer_name", "amount", "order_date"]

def create_sources():
    """Creates the sample source files (CSV and JSON) and an empty warehouse."""
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)

    # January orders: messy data (extra spaces, duplicate, missing amount)
    with open("orders_jan.csv", "w") as f:
        f.write("order_id,customer_name,amount,order_date\n"
                "1,  asha rao ,100,2026-01-05\n"
                "2,RAVI KUMAR,200,2026-01-12\n"
                "2,RAVI KUMAR,200,2026-01-12\n"
                "3,neha shah,,2026-01-20\n")

    # February orders: negative amount, missing name, text amount, bad date
    with open("orders_feb.csv", "w") as f:
        f.write("order_id,customer_name,amount,order_date\n"
                "4,john mathew,300,2026-02-03\n"
                "5,priya nair,-50,2026-02-10\n"
                "6,amit shah,450,2026-02-18\n"
                "7,,120,2026-02-20\n"
                "8,sara khan,abc,2026-02-22\n"
                "9,tom paul,250,2026-13-45\n")

    # March orders: newly arriving data (order 6 was already loaded)
    with open("orders_mar.csv", "w") as f:
        f.write("order_id,customer_name,amount,order_date\n"
                "6,amit shah,450,2026-02-18\n"
                "13,rohan das,500,2026-03-02\n"
                "14,meena iyer,175,2026-03-05\n")

    # JSON orders: nested customer object and extra fields
    json_orders = [
        {"order_id": 10, "customer": {"name": "karan mehta", "city": "Pune"},
         "amount": 150, "order_date": "2026-02-25", "notes": "gift"},
        {"order_id": 11, "customer": {"name": "lina dsouza", "city": "Mumbai"},
         "amount": 275, "order_date": "2026-02-27", "notes": "express"},
        {"order_id": 12, "customer": {"name": "ravi teja", "city": "Delhi"},
         "amount": None, "order_date": "2026-02-28", "notes": "invalid"},
    ]
    with open("orders.json", "w") as f:
        json.dump(json_orders, f, indent=2)
    print("Source files created: orders_jan.csv, orders_feb.csv, orders_mar.csv, orders.json")

create_sources()

def extract_csv(path):
    return pd.read_csv(path)


def extract_multiple_csv(file_list):
    frames = [pd.read_csv(f) for f in file_list if os.path.exists(f)]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def extract_json(path):
    with open(path) as f:
        raw = json.load(f)
    df = pd.json_normalize(raw)                      # flatten nested fields
    df = df[["order_id", "customer.name", "amount", "order_date"]]   # select fields
    return df.rename(columns={"customer.name": "customer_name"})

def find_reasons(df):
    """Returns a text reason for every invalid record ('' if the record is valid)."""
    reasons = pd.Series("", index=df.index, dtype=object)

    def add(mask, text):
        mask = pd.Series(mask, index=df.index).fillna(False).astype(bool)
        for i in df.index[mask]:
            reasons[i] = (reasons[i] + "; " if reasons[i] else "") + text

    order_id = pd.to_numeric(df["order_id"], errors="coerce")
    name = df["customer_name"].astype("string").str.strip()
    amount = pd.to_numeric(df["amount"], errors="coerce")
    dates = pd.to_datetime(df["order_date"], errors="coerce")

    add(order_id.isna(), "invalid order_id")
    add(name.isna() | (name == ""), "missing customer_name")
    add(df["amount"].isna(), "missing amount")
    add(amount.isna() & df["amount"].notna(), "non-numeric amount")
    add(amount <= 0, "amount not positive")
    add(dates.isna(), "invalid order_date")
    add(df.duplicated("order_id", keep="first") & order_id.notna(), "duplicate order_id")
    return reasons


def clean_invalid(df):
    """Splits the data into (valid_records, rejected_records_with_reason)."""
    reasons = find_reasons(df)
    valid = df[reasons == ""].copy()
    rejected = df[reasons != ""].copy()
    rejected["reason"] = reasons[reasons != ""]
    return valid, rejected

def transform(df):
    out = df.copy()
    out["order_id"] = pd.to_numeric(out["order_id"]).astype(int)
    out["customer_name"] = out["customer_name"].str.strip().str.title()
    out["amount"] = pd.to_numeric(out["amount"]).astype(float)
    out["order_date"] = pd.to_datetime(out["order_date"])
    out["order_month"] = out["order_date"].dt.strftime("%Y-%m")
    out["order_date"] = out["order_date"].dt.strftime("%Y-%m-%d")
    out["amount_category"] = out["amount"].apply(lambda a: "high" if a >= 300 else "low")
    return out.reset_index(drop=True)

def validate(df, required_cols):
    """Raises ValueError if the data is not fit to be loaded."""
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"missing columns: {missing}")
    if df.empty:
        raise ValueError("no records to load")

    errors = []
    for c in required_cols:
        n = int(df[c].isna().sum())
        if n:
            errors.append(f"{n} null value(s) in '{c}'")
    if df["order_id"].duplicated().any():
        errors.append("duplicate order_id values")
    amount = pd.to_numeric(df["amount"], errors="coerce")
    if (amount.isna() | (amount <= 0)).any():
        errors.append("invalid amount values")
    if pd.to_datetime(df["order_date"], errors="coerce").isna().any():
        errors.append("invalid order_date values")

    if errors:
        raise ValueError("; ".join(errors))
    print(f"Validation passed: {len(df)} records ready to load.")
    return True

def print_header(title):
    print("\n" + "=" * 70 + "\n" + title + "\n" + "=" * 70)


def read_table(sql):
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


def full_load(df, table_name):
    """Loads the whole dataset (replaces the table)."""
    conn = sqlite3.connect(DB_FILE)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    print(f"Full load: {len(df)} record(s) written to '{table_name}'.")


def incremental_load(df, table_name, key_col):
    """Inserts only the records whose key is not already in the table."""
    conn = sqlite3.connect(DB_FILE)
    try:
        existing = pd.read_sql(f"SELECT {key_col} FROM {table_name}", conn)[key_col].tolist()
    except Exception:
        existing = []                                   # table does not exist yet
    new_rows = df[~df[key_col].isin(existing)]
    if not new_rows.empty:
        new_rows.to_sql(table_name, conn, if_exists="append", index=False)
    print(f"Incremental load: {len(new_rows)} new record(s) inserted, "
          f"{len(df) - len(new_rows)} already-loaded record(s) skipped.")
    conn.close()

print_header("TASK 1: Single CSV -> clean -> transform -> database")
raw_jan = extract_csv("orders_jan.csv")
print("Extracted (raw):\n", raw_jan)

valid_jan, rejected_jan = clean_invalid(raw_jan)
print("\nRejected records:\n", rejected_jan)

jan_ready = transform(valid_jan)
print("\nAfter cleaning and transformation:\n", jan_ready)

validate(jan_ready, REQUIRED)
full_load(jan_ready, "jan_orders")
print("\nStored in table 'jan_orders':\n", read_table("SELECT * FROM jan_orders"))

print("\n" + "=" * 40)
print("Submitted by:", STUDENT_NAME)
print("=" * 40)

print_header("TASK 2: Multiple CSV files -> one dataset")
csv_files = ["orders_jan.csv", "orders_feb.csv"]
for f in csv_files:
    print(f, "->", len(extract_csv(f)), "rows")

combined = extract_multiple_csv(csv_files)
print("\nCombined dataset:", len(combined), "rows\n", combined)

print("\n" + "=" * 40)
print("Submitted by:", STUDENT_NAME)
print("=" * 40)

print_header("TASK 3: JSON -> transform selected fields -> relational database")
json_raw = extract_json("orders.json")
print("Selected fields from JSON:\n", json_raw)

json_valid, json_rejected = clean_invalid(json_raw)
print("\nRejected records:\n", json_rejected)

json_ready = transform(json_valid)
validate(json_ready, REQUIRED)
full_load(json_ready, "json_orders")
print("\nStored in table 'json_orders':\n", read_table("SELECT * FROM json_orders"))

print("\n" + "=" * 40)
print("Submitted by:", STUDENT_NAME)
print("=" * 40)

print_header("TASK 4: Identify and remove invalid records")
valid_all, rejected_all = clean_invalid(combined)
print(f"Total: {len(combined)} | Valid: {len(valid_all)} | Invalid: {len(rejected_all)}")
print("\nInvalid records removed (with reason):\n", rejected_all)

print("\n" + "=" * 40)
print("Submitted by:", STUDENT_NAME)
print("=" * 40)

print_header("TASK 5: Data validation before loading")
try:
    validate(combined, REQUIRED)                 # raw data -> should FAIL
except ValueError as err:
    print("Validation FAILED on raw data ->", err)

clean_ready = transform(valid_all)
validate(clean_ready, REQUIRED)                  # cleaned data -> passes

print("\n" + "=" * 40)
print("Submitted by:", STUDENT_NAME)
print("=" * 40)

print_header("TASK 6: Incremental loading")
print("Run 1 (first load):")
incremental_load(clean_ready, "orders", "order_id")
print("Run 2 (same data again):")
incremental_load(clean_ready, "orders", "order_id")

print("\nNew data arrives (orders_mar.csv + orders.json):")
new_batch = pd.concat([extract_csv("orders_mar.csv"), extract_json("orders.json")],
                      ignore_index=True)
new_valid, new_rejected = clean_invalid(new_batch)
print("Invalid records in new batch:\n", new_rejected)
new_ready = transform(new_valid)
validate(new_ready, REQUIRED)
print("Run 3 (new batch):")
incremental_load(new_ready, "orders", "order_id")

print("\nFinal 'orders' table:\n", read_table("SELECT * FROM orders ORDER BY order_id"))
print("\nRecords per table:")
for t in ["jan_orders", "json_orders", "orders"]:
    print(f"  {t}: {read_table(f'SELECT COUNT(*) AS n FROM {t}')['n'][0]}")

    print("\n" + "=" * 40)
print("Submitted by:", STUDENT_NAME)
print("=" * 40)
```

---

## 4. Output
```text
Source files created: orders_jan.csv, orders_feb.csv, orders_mar.csv, orders.json

======================================================================
TASK 1: Single CSV -> clean -> transform -> database
======================================================================
Extracted (raw):
    order_id customer_name  amount  order_date
0         1     asha rao    100.0  2026-01-05
1         2    RAVI KUMAR   200.0  2026-01-12
2         2    RAVI KUMAR   200.0  2026-01-12
3         3     neha shah     NaN  2026-01-20

Rejected records:
    order_id customer_name  amount  order_date              reason
2         2    RAVI KUMAR   200.0  2026-01-12  duplicate order_id
3         3     neha shah     NaN  2026-01-20      missing amount

After cleaning and transformation:
    order_id customer_name  amount  order_date order_month amount_category
0         1      Asha Rao   100.0  2026-01-05     2026-01             low
1         2    Ravi Kumar   200.0  2026-01-12     2026-01             low
Validation passed: 2 records ready to load.
Full load: 2 record(s) written to 'jan_orders'.

Stored in table 'jan_orders':
    order_id customer_name  amount  order_date order_month amount_category
0         1      Asha Rao   100.0  2026-01-05     2026-01             low
1         2    Ravi Kumar   200.0  2026-01-12     2026-01             low

========================================
Submitted by: Wali Momin
========================================

======================================================================
TASK 2: Multiple CSV files -> one dataset
======================================================================
orders_jan.csv -> 4 rows
orders_feb.csv -> 6 rows

Combined dataset: 10 rows
    order_id customer_name amount  order_date
0         1     asha rao   100.0  2026-01-05
1         2    RAVI KUMAR  200.0  2026-01-12
2         2    RAVI KUMAR  200.0  2026-01-12
3         3     neha shah    NaN  2026-01-20
4         4   john mathew    300  2026-02-03
5         5    priya nair    -50  2026-02-10
6         6     amit shah    450  2026-02-18
7         7           NaN    120  2026-02-20
8         8     sara khan    abc  2026-02-22
9         9      tom paul    250  2026-13-45

========================================
Submitted by: Wali Momin
========================================

======================================================================
TASK 3: JSON -> transform selected fields -> relational database
======================================================================
Selected fields from JSON:
    order_id customer_name  amount  order_date
0        10   karan mehta   150.0  2026-02-25
1        11   lina dsouza   275.0  2026-02-27
2        12     ravi teja     NaN  2026-02-28

Rejected records:
    order_id customer_name  amount  order_date          reason
2        12     ravi teja     NaN  2026-02-28  missing amount
Validation passed: 2 records ready to load.
Full load: 2 record(s) written to 'json_orders'.

Stored in table 'json_orders':
    order_id customer_name  amount  order_date order_month amount_category
0        10   Karan Mehta   150.0  2026-02-25     2026-02             low
1        11   Lina Dsouza   275.0  2026-02-27     2026-02             low

========================================
Submitted by: Wali Momin
========================================

======================================================================
TASK 4: Identify and remove invalid records
======================================================================
Total: 10 | Valid: 4 | Invalid: 6

Invalid records removed (with reason):
    order_id customer_name amount  order_date                 reason
2         2    RAVI KUMAR  200.0  2026-01-12     duplicate order_id
3         3     neha shah    NaN  2026-01-20         missing amount
5         5    priya nair    -50  2026-02-10    amount not positive
7         7           NaN    120  2026-02-20  missing customer_name
8         8     sara khan    abc  2026-02-22     non-numeric amount
9         9      tom paul    250  2026-13-45     invalid order_date

========================================
Submitted by: Wali Momin
========================================

======================================================================
TASK 5: Data validation before loading
======================================================================
Validation FAILED on raw data -> 1 null value(s) in 'customer_name'; 1 null value(s) in 'amount'; duplicate order_id values; invalid amount values; invalid order_date values
Validation passed: 4 records ready to load.

========================================
Submitted by: Wali Momin
========================================

======================================================================
TASK 6: Incremental loading
======================================================================
Run 1 (first load):
Incremental load: 4 new record(s) inserted, 0 already-loaded record(s) skipped.
Run 2 (same data again):
Incremental load: 0 new record(s) inserted, 4 already-loaded record(s) skipped.

New data arrives (orders_mar.csv + orders.json):
Invalid records in new batch:
    order_id customer_name  amount  order_date          reason
5        12     ravi teja     NaN  2026-02-28  missing amount
Validation passed: 5 records ready to load.
Run 3 (new batch):
Incremental load: 4 new record(s) inserted, 1 already-loaded record(s) skipped.

Final 'orders' table:
    order_id customer_name  amount  order_date order_month amount_category
0         1      Asha Rao   100.0  2026-01-05     2026-01             low
1         2    Ravi Kumar   200.0  2026-01-12     2026-01             low
2         4   John Mathew   300.0  2026-02-03     2026-02            high
3         6     Amit Shah   450.0  2026-02-18     2026-02            high
4        10   Karan Mehta   150.0  2026-02-25     2026-02             low
5        11   Lina Dsouza   275.0  2026-02-27     2026-02             low
6        13     Rohan Das   500.0  2026-03-02     2026-03            high
7        14    Meena Iyer   175.0  2026-03-05     2026-03             low

Records per table:
  jan_orders: 2

========================================
  json_orders: 2

========================================
  orders: 8

========================================
Submitted by: Wali Momin
========================================
```

---

## 5. Conclusion
Successfully implemented an end-to-end Python ETL pipeline. Demonstrated robust multi-source extraction, strict validation filters that safely quarantined corrupted records, and verified idempotent incremental database loading.
