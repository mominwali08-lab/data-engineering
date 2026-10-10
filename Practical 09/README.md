# Practical 9: Enterprise E-Commerce ETL Pipeline with Relational Integrity & Reporting

**Author:** Wali Momin

---

## 1. Aim
To implement a production-grade ETL pipeline handling relational e-commerce data (Customers, Products, Orders, Payments), enforcing primary/foreign key integrity, executing incremental batch loads, and generating executive business intelligence reports.

---

## 2. Brief Theory
Comprehensive enterprise data engineering bridges transactional ingestion and analytical reporting:
- **Relational Integrity in ETL:**
  - Verifying entity existence before insertion (e.g. Orders must reference valid Customers and Products).
  - Quarantining orphan records and duplicate transaction identifiers.
- **Multi-Table Relational Schema:**
  - Four interconnected tables: `customers`, `products`, `orders`, `payments`.
- **Incremental Loading:**
  - Efficiently loading historical datasets followed by newly arriving delta batches without reprocessing existing keys.
- **Business Intelligence Reporting:**
  - Aggregating transactional facts: Revenue by city and product category, payment method breakdown, and top-value customer analysis using SQL analytical queries.

---

## 3. Code
```python
import os
import sqlite3
import pandas as pd

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)

STUDENT_NAME = "Wali Momin"
DB_FILE = "ecommerce.db"

def create_sources():
    """Creates the sample CSV files: historical batch and newly arriving batch."""
    files = {
        # ---------------- HISTORICAL BATCH ----------------
        "customers_historical.csv": """customer_id,name,email,city
1,  asha rao ,Asha@Mail.com,mumbai
2,RAVI KUMAR,ravi@mail.com,Delhi
2,RAVI KUMAR,ravi@mail.com,Delhi
3,neha shah,NEHA@mail.com,pune
5,tom paul,tom_at_mail.com,Chennai
""",
        "products_historical.csv": """product_id,product_name,category,price
101,Laptop,Electronics,55000
102,Phone,Electronics,20000
103,Headphones,Accessories,1500
104,Desk Lamp,Home,-300
""",
        "orders_historical.csv": """order_id,customer_id,product_id,order_date,quantity
1,1,101,2026-01-05,1
2,2,102,2026-01-06,2
3,3,103,2026-01-09,3
3,3,103,2026-01-09,3
4,1,104,2026-01-10,1
5,2,102,2026-13-01,1
""",
        "payments_historical.csv": """payment_id,order_id,amount,status,payment_date
1001,1,55000,paid,2026-01-05
1002,2,40000,PAID,2026-01-06
1003,3,4500,pending,2026-01-09
1004,4,300,paid,2026-01-10
1005,99,700,paid,2026-01-11
""",
        # ---------------- NEWLY ARRIVING BATCH ----------------
        "customers_new.csv": """customer_id,name,email,city
2,Ravi Kumar,ravi@mail.com,Delhi
4,priya nair,priya@mail.com,bengaluru
""",
        "products_new.csv": """product_id,product_name,category,price
103,Headphones,Accessories,1500
105,Backpack,Accessories,2500
""",
        "orders_new.csv": """order_id,customer_id,product_id,order_date,quantity
2,2,102,2026-01-06,2
6,4,101,2026-02-02,1
7,1,105,2026-02-04,2
7,1,105,2026-02-04,2
8,3,102,2026-02-10,0
9,9,101,2026-02-11,1
10,2,,2026-02-12,1
""",
        "payments_new.csv": """payment_id,order_id,amount,status,payment_date
1006,6,55000,Paid,2026-02-02
1007,7,5000,Paid,2026-02-04
1007,7,5000,Paid,2026-02-04
1008,8,20000,Paid,2026-02-10
""",
    }
    for name, text in files.items():
        with open(name, "w") as f:
            f.write(text)
    print("Created", len(files), "source CSV files (historical + new batch).")

create_sources()

def init_db():
    """Creates an empty database with 4 related tables (primary and foreign keys)."""
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    conn = sqlite3.connect(DB_FILE)
    conn.executescript("""
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            name        TEXT NOT NULL,
            email       TEXT NOT NULL,
            city        TEXT
        );
        CREATE TABLE products (
            product_id   INTEGER PRIMARY KEY,
            product_name TEXT NOT NULL,
            category     TEXT,
            price        REAL NOT NULL CHECK (price > 0)
        );
        CREATE TABLE orders (
            order_id    INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
            product_id  INTEGER NOT NULL REFERENCES products(product_id),
            order_date  TEXT NOT NULL,
            quantity    INTEGER NOT NULL CHECK (quantity > 0)
        );
        CREATE TABLE payments (
            payment_id   INTEGER PRIMARY KEY,
            order_id     INTEGER NOT NULL REFERENCES orders(order_id),
            amount       REAL NOT NULL CHECK (amount > 0),
            status       TEXT NOT NULL,
            payment_date TEXT
        );
    """)
    conn.commit()
    conn.close()
    print("Database created: customers, products, orders, payments")

init_db()

def print_header(title):
    print("\n" + "=" * 70 + "\n" + title + "\n" + "=" * 70)


def run_sql(sql):
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


def db_ids(table, col):
    """Keys already stored in the database (empty set if the table is empty)."""
    return set(run_sql(f"SELECT {col} FROM {table}")[col])

def split_valid(df, checks):
    """checks = list of (boolean mask, reason). Returns (valid_rows, rejected_rows)."""
    reasons = pd.Series("", index=df.index, dtype=object)
    for mask, text in checks:
        mask = pd.Series(mask, index=df.index).fillna(False).astype(bool)
        for i in df.index[mask]:
            reasons[i] = (reasons[i] + "; " if reasons[i] else "") + text
    valid = df[reasons == ""].copy()
    rejected = df[reasons != ""].copy()
    rejected["reason"] = reasons[reasons != ""]
    return valid, rejected


def clean_customers(df):
    cid = pd.to_numeric(df["customer_id"], errors="coerce")
    name = df["name"].astype("string").str.strip()
    email = df["email"].astype("string").str.strip()
    valid, rejected = split_valid(df, [
        (cid.isna(), "invalid customer_id"),
        (name.isna() | (name == ""), "missing name"),
        (~email.str.contains("@", na=False), "invalid email"),
        (df.duplicated("customer_id") & cid.notna(), "duplicate customer_id"),
    ])
    valid["customer_id"] = pd.to_numeric(valid["customer_id"]).astype(int)
    valid["name"] = valid["name"].str.strip().str.title()
    valid["email"] = valid["email"].str.strip().str.lower()
    valid["city"] = valid["city"].str.strip().str.title()
    return valid.reset_index(drop=True), rejected


def clean_products(df):
    pid = pd.to_numeric(df["product_id"], errors="coerce")
    price = pd.to_numeric(df["price"], errors="coerce")
    name = df["product_name"].astype("string").str.strip()
    valid, rejected = split_valid(df, [
        (pid.isna(), "invalid product_id"),
        (name.isna() | (name == ""), "missing product_name"),
        (price.isna() | (price <= 0), "invalid price"),
        (df.duplicated("product_id") & pid.notna(), "duplicate product_id"),
    ])
    valid["product_id"] = pd.to_numeric(valid["product_id"]).astype(int)
    valid["product_name"] = valid["product_name"].str.strip().str.title()
    valid["category"] = valid["category"].str.strip().str.title()
    valid["price"] = pd.to_numeric(valid["price"]).astype(float)
    return valid.reset_index(drop=True), rejected


def clean_orders(df, known_customers, known_products):
    oid = pd.to_numeric(df["order_id"], errors="coerce")
    cid = pd.to_numeric(df["customer_id"], errors="coerce")
    pid = pd.to_numeric(df["product_id"], errors="coerce")
    qty = pd.to_numeric(df["quantity"], errors="coerce")
    dates = pd.to_datetime(df["order_date"], errors="coerce")
    valid, rejected = split_valid(df, [
        (oid.isna(), "invalid order_id"),
        (cid.isna(), "missing customer_id"),
        (pid.isna(), "missing product_id"),
        (qty.isna() | (qty <= 0), "invalid quantity"),
        (dates.isna(), "invalid order_date"),
        (cid.notna() & ~cid.isin(known_customers), "customer not found"),
        (pid.notna() & ~pid.isin(known_products), "product not found"),
        (df.duplicated("order_id") & oid.notna(), "duplicate order_id"),
    ])
    for col in ["order_id", "customer_id", "product_id", "quantity"]:
        valid[col] = pd.to_numeric(valid[col]).astype(int)
    valid["order_date"] = pd.to_datetime(valid["order_date"]).dt.strftime("%Y-%m-%d")
    return valid.reset_index(drop=True), rejected


def clean_payments(df, known_orders):
    pay_id = pd.to_numeric(df["payment_id"], errors="coerce")
    oid = pd.to_numeric(df["order_id"], errors="coerce")
    amount = pd.to_numeric(df["amount"], errors="coerce")
    status = df["status"].astype("string").str.strip().str.title()
    valid, rejected = split_valid(df, [
        (pay_id.isna(), "invalid payment_id"),
        (oid.isna(), "invalid order_id"),
        (amount.isna() | (amount <= 0), "invalid amount"),
        (~status.isin(["Paid", "Pending", "Failed", "Refunded"]), "invalid status"),
        (oid.notna() & ~oid.isin(known_orders), "order not found"),
        (df.duplicated("payment_id") & pay_id.notna(), "duplicate payment_id"),
    ])
    for col in ["payment_id", "order_id"]:
        valid[col] = pd.to_numeric(valid[col]).astype(int)
    valid["amount"] = pd.to_numeric(valid["amount"]).astype(float)
    valid["status"] = valid["status"].str.strip().str.title()
    valid["payment_date"] = pd.to_datetime(valid["payment_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return valid.reset_index(drop=True), rejected

def incremental_load(df, table, key):
    """Inserts only the rows whose key is not already in the table."""
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    existing = set(pd.read_sql(f"SELECT {key} FROM {table}", conn)[key])
    new_rows = df[~df[key].isin(existing)]
    if len(new_rows):
        new_rows.to_sql(table, conn, if_exists="append", index=False)
    conn.commit()
    conn.close()
    return len(new_rows), len(df) - len(new_rows)

def run_pipeline(label, customers_file, products_file, orders_file, payments_file):
    """EXTRACT (CSV -> Pandas) -> CLEAN -> LOAD (SQL database, incremental)."""
    print_header(f"PIPELINE RUN: {label}")

    # ---------- EXTRACT ----------
    raw = {
        "customers": pd.read_csv(customers_file),
        "products": pd.read_csv(products_file),
        "orders": pd.read_csv(orders_file),
        "payments": pd.read_csv(payments_file),
    }

    # ---------- CLEAN (parents first, so child records can be checked) ----------
    customers, rej_customers = clean_customers(raw["customers"])
    products, rej_products = clean_products(raw["products"])

    known_customers = db_ids("customers", "customer_id") | set(customers["customer_id"])
    known_products = db_ids("products", "product_id") | set(products["product_id"])
    orders, rej_orders = clean_orders(raw["orders"], known_customers, known_products)

    known_orders = db_ids("orders", "order_id") | set(orders["order_id"])
    payments, rej_payments = clean_payments(raw["payments"], known_orders)

    rejected = {"customers": rej_customers, "products": rej_products,
                "orders": rej_orders, "payments": rej_payments}
    clean = {"customers": customers, "products": products,
             "orders": orders, "payments": payments}
    keys = {"customers": "customer_id", "products": "product_id",
            "orders": "order_id", "payments": "payment_id"}

    # ---------- LOAD (incremental, parents first) ----------
    summary = []
    for table in ["customers", "products", "orders", "payments"]:
        inserted, skipped = incremental_load(clean[table], table, keys[table])
        summary.append({"table": table, "raw_rows": len(raw[table]),
                        "rejected": len(rejected[table]), "valid": len(clean[table]),
                        "inserted": inserted, "already_loaded": skipped})

    for table, rej in rejected.items():
        if len(rej):
            print(f"\nRejected {table} records:")
            print(rej.to_string(index=False))
    print("\nLoad summary:")
    print(pd.DataFrame(summary).to_string(index=False))
    return summary

def business_report(title):
    print_header(f"REPORT: {title}")

    print("1. Revenue by city and category (paid orders):")
    print(run_sql("""
        SELECT c.city, p.category,
               COUNT(DISTINCT o.order_id) AS orders,
               SUM(pay.amount)            AS revenue
        FROM orders o
        JOIN customers c ON c.customer_id = o.customer_id
        JOIN products  p ON p.product_id  = o.product_id
        JOIN payments pay ON pay.order_id = o.order_id
        WHERE pay.status = 'Paid'
        GROUP BY c.city, p.category
        ORDER BY revenue DESC
    """).to_string(index=False))

    print("\n2. Payment status summary:")
    print(run_sql("""
        SELECT status, COUNT(*) AS payments, SUM(amount) AS total_amount
        FROM payments GROUP BY status ORDER BY total_amount DESC
    """).to_string(index=False))

    print("\n3. Top customers by amount paid:")
    print(run_sql("""
        SELECT c.name, c.city, SUM(pay.amount) AS total_paid
        FROM payments pay
        JOIN orders o    ON o.order_id    = pay.order_id
        JOIN customers c ON c.customer_id = o.customer_id
        WHERE pay.status = 'Paid'
        GROUP BY c.customer_id ORDER BY total_paid DESC
    """).to_string(index=False))

    print("\n4. Monthly revenue (paid):")
    print(run_sql("""
        SELECT substr(o.order_date, 1, 7) AS month, SUM(pay.amount) AS revenue
        FROM payments pay JOIN orders o ON o.order_id = pay.order_id
        WHERE pay.status = 'Paid'
        GROUP BY month ORDER BY month
    """).to_string(index=False))


def show_database():
    for table in ["customers", "products", "orders", "payments"]:
        print(f"\nTable '{table}':")
        print(run_sql(f"SELECT * FROM {table}").to_string(index=False))

print_header("TASK 1: End-to-end pipeline  CSV -> Pandas -> Cleaning -> SQL database -> Report")
print("Raw historical files (before cleaning):")
for f in ["customers_historical.csv", "products_historical.csv",
          "orders_historical.csv", "payments_historical.csv"]:
    print(f"\n{f}:")
    print(pd.read_csv(f).to_string(index=False))

run_pipeline("HISTORICAL DATA (first load)",
             "customers_historical.csv", "products_historical.csv",
             "orders_historical.csv", "payments_historical.csv")
business_report("after historical load")

print("\n" + "=" * 40)
print("Submitted by:", STUDENT_NAME)
print("=" * 40)

print_header("TASK 2: E-commerce dataset - customers, orders, products, payments")
print("Cleaned data stored in the 4 related tables of the SQL database:")
show_database()

print("\nTable structure (columns, primary keys):")
for table in ["customers", "products", "orders", "payments"]:
    info = run_sql(f"PRAGMA table_info({table})")[["name", "type", "pk"]]
    print(f"\n{table}:")
    print(info.to_string(index=False))

    print("\n" + "=" * 40)
print("Submitted by:", STUDENT_NAME)
print("=" * 40)

print_header("TASK 3: Historical + newly arriving data (incremental load)")
before = {t: len(run_sql(f"SELECT * FROM {t}")) for t in ["customers", "products", "orders", "payments"]}

print("New files that have just arrived:")
for f in ["customers_new.csv", "products_new.csv", "orders_new.csv", "payments_new.csv"]:
    print(f"\n{f}:")
    print(pd.read_csv(f).to_string(index=False))

run_pipeline("NEWLY ARRIVING DATA (incremental load)",
             "customers_new.csv", "products_new.csv",
             "orders_new.csv", "payments_new.csv")

after = {t: len(run_sql(f"SELECT * FROM {t}")) for t in before}
print("\nRows in each table, before and after the new data:")
print(pd.DataFrame({"before": before, "after": after}))

print("\nIntegrity check - orders without a customer or product (should be 0):")
print(run_sql("""
    SELECT COUNT(*) AS orphan_orders FROM orders
    WHERE customer_id NOT IN (SELECT customer_id FROM customers)
       OR product_id  NOT IN (SELECT product_id  FROM products)
""").to_string(index=False))

business_report("after historical + new data")

print("\n" + "=" * 40)
print("Submitted by:", STUDENT_NAME)
print("=" * 40)
```

---

## 4. Output
```text
Created 8 source CSV files (historical + new batch).

Database created: customers, products, orders, payments

======================================================================
TASK 1: End-to-end pipeline  CSV -> Pandas -> Cleaning -> SQL database -> Report
======================================================================
Raw historical files (before cleaning):

customers_historical.csv:
 customer_id        name           email    city
           1   asha rao    Asha@Mail.com  mumbai
           2  RAVI KUMAR   ravi@mail.com   Delhi
           2  RAVI KUMAR   ravi@mail.com   Delhi
           3   neha shah   NEHA@mail.com    pune
           5    tom paul tom_at_mail.com Chennai

products_historical.csv:
 product_id product_name    category  price
        101       Laptop Electronics  55000
        102        Phone Electronics  20000
        103   Headphones Accessories   1500
        104    Desk Lamp        Home   -300

orders_historical.csv:
 order_id  customer_id  product_id order_date  quantity
        1            1         101 2026-01-05         1
        2            2         102 2026-01-06         2
        3            3         103 2026-01-09         3
        3            3         103 2026-01-09         3
        4            1         104 2026-01-10         1
        5            2         102 2026-13-01         1

payments_historical.csv:
 payment_id  order_id  amount  status payment_date
       1001         1   55000    paid   2026-01-05
       1002         2   40000    PAID   2026-01-06
       1003         3    4500 pending   2026-01-09
       1004         4     300    paid   2026-01-10
       1005        99     700    paid   2026-01-11

======================================================================
PIPELINE RUN: HISTORICAL DATA (first load)
======================================================================

Rejected customers records:
 customer_id       name           email    city                reason
           2 RAVI KUMAR   ravi@mail.com   Delhi duplicate customer_id
           5   tom paul tom_at_mail.com Chennai         invalid email

Rejected products records:
 product_id product_name category  price        reason
        104    Desk Lamp     Home   -300 invalid price

Rejected orders records:
 order_id  customer_id  product_id order_date  quantity             reason
        3            3         103 2026-01-09         3 duplicate order_id
        4            1         104 2026-01-10         1  product not found
        5            2         102 2026-13-01         1 invalid order_date

Rejected payments records:
 payment_id  order_id  amount status payment_date          reason
       1004         4     300   paid   2026-01-10 order not found
       1005        99     700   paid   2026-01-11 order not found

Load summary:
    table  raw_rows  rejected  valid  inserted  already_loaded
customers         5         2      3         3               0
 products         4         1      3         3               0
   orders         6         3      3         3               0
 payments         5         2      3         3               0

======================================================================
REPORT: after historical load
======================================================================
1. Revenue by city and category (paid orders):
  city    category  orders  revenue
Mumbai Electronics       1  55000.0
 Delhi Electronics       1  40000.0

2. Payment status summary:
 status  payments  total_amount
   Paid         2       95000.0
Pending         1        4500.0

3. Top customers by amount paid:
      name   city  total_paid
  Asha Rao Mumbai     55000.0
Ravi Kumar  Delhi     40000.0

4. Monthly revenue (paid):
  month  revenue
2026-01  95000.0

========================================
Submitted by: Wali Momin
========================================

======================================================================
TASK 2: E-commerce dataset - customers, orders, products, payments
======================================================================
Cleaned data stored in the 4 related tables of the SQL database:

Table 'customers':
 customer_id       name         email   city
           1   Asha Rao asha@mail.com Mumbai
           2 Ravi Kumar ravi@mail.com  Delhi
           3  Neha Shah neha@mail.com   Pune

Table 'products':
 product_id product_name    category   price
        101       Laptop Electronics 55000.0
        102        Phone Electronics 20000.0
        103   Headphones Accessories  1500.0

Table 'orders':
 order_id  customer_id  product_id order_date  quantity
        1            1         101 2026-01-05         1
        2            2         102 2026-01-06         2
        3            3         103 2026-01-09         3

Table 'payments':
 payment_id  order_id  amount  status payment_date
       1001         1 55000.0    Paid   2026-01-05
       1002         2 40000.0    Paid   2026-01-06
       1003         3  4500.0 Pending   2026-01-09

Table structure (columns, primary keys):

customers:
       name    type  pk
customer_id INTEGER   1
       name    TEXT   0
      email    TEXT   0
       city    TEXT   0

========================================

products:
        name    type  pk
  product_id INTEGER   1
product_name    TEXT   0
    category    TEXT   0
       price    REAL   0

========================================

orders:
       name    type  pk
   order_id INTEGER   1
customer_id INTEGER   0
 product_id INTEGER   0
 order_date    TEXT   0
   quantity INTEGER   0

========================================

payments:
        name    type  pk
  payment_id INTEGER   1
    order_id INTEGER   0
      amount    REAL   0
      status    TEXT   0
payment_date    TEXT   0

========================================
Submitted by: Wali Momin
========================================

======================================================================
TASK 3: Historical + newly arriving data (incremental load)
======================================================================
New files that have just arrived:

customers_new.csv:
 customer_id       name          email      city
           2 Ravi Kumar  ravi@mail.com     Delhi
           4 priya nair priya@mail.com bengaluru

products_new.csv:
 product_id product_name    category  price
        103   Headphones Accessories   1500
        105     Backpack Accessories   2500

orders_new.csv:
 order_id  customer_id  product_id order_date  quantity
        2            2       102.0 2026-01-06         2
        6            4       101.0 2026-02-02         1
        7            1       105.0 2026-02-04         2
        7            1       105.0 2026-02-04         2
        8            3       102.0 2026-02-10         0
        9            9       101.0 2026-02-11         1
       10            2         NaN 2026-02-12         1

payments_new.csv:
 payment_id  order_id  amount status payment_date
       1006         6   55000   Paid   2026-02-02
       1007         7    5000   Paid   2026-02-04
       1007         7    5000   Paid   2026-02-04
       1008         8   20000   Paid   2026-02-10

======================================================================
PIPELINE RUN: NEWLY ARRIVING DATA (incremental load)
======================================================================

Rejected orders records:
 order_id  customer_id  product_id order_date  quantity             reason
        7            1       105.0 2026-02-04         2 duplicate order_id
        8            3       102.0 2026-02-10         0   invalid quantity
        9            9       101.0 2026-02-11         1 customer not found
       10            2         NaN 2026-02-12         1 missing product_id

Rejected payments records:
 payment_id  order_id  amount status payment_date               reason
       1007         7    5000   Paid   2026-02-04 duplicate payment_id
       1008         8   20000   Paid   2026-02-10      order not found

Load summary:
    table  raw_rows  rejected  valid  inserted  already_loaded
customers         2         0      2         1               1
 products         2         0      2         1               1
   orders         7         4      3         2               1
 payments         4         2      2         2               0

Rows in each table, before and after the new data:
           before  after
customers       3      4
products        3      4
orders          3      5
payments        3      5

Integrity check - orders without a customer or product (should be 0):
 orphan_orders
             0

======================================================================
REPORT: after historical + new data
======================================================================
1. Revenue by city and category (paid orders):
     city    category  orders  revenue
Bengaluru Electronics       1  55000.0
   Mumbai Electronics       1  55000.0
    Delhi Electronics       1  40000.0
   Mumbai Accessories       1   5000.0

2. Payment status summary:
 status  payments  total_amount
   Paid         4      155000.0
Pending         1        4500.0

3. Top customers by amount paid:
      name      city  total_paid
  Asha Rao    Mumbai     60000.0
Priya Nair Bengaluru     55000.0
Ravi Kumar     Delhi     40000.0

4. Monthly revenue (paid):
  month  revenue
2026-01  95000.0
2026-02  60000.0

========================================
Submitted by: Wali Momin
========================================
```

---

## 5. Conclusion
Successfully engineered a multi-entity relational ETL pipeline in Python and SQLite. Demonstrated foreign key validation, orphan record isolation, incremental data loading, and executive BI reporting queries.
