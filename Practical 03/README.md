# Practical 3: NoSQL Database Management with MongoDB

**Author:** Wali Momin

---

## 1. Aim
To install, configure, and operate a MongoDB NoSQL database service, and perform document-oriented CRUD operations (insert single/multiple documents, retrieve, and format queries) using the MongoDB shell (`mongosh`).

---

## 2. Brief Theory
NoSQL (Not Only SQL) databases are designed for high scalability, flexible schemas, and rapid application development:
- **Document Model (BSON/JSON):** MongoDB stores data as documents in binary JSON (BSON) format, allowing dynamic schemas and nested objects/arrays.
- **Collections:** High-level grouping of documents, analogous to tables in relational databases.
- **Key MongoDB Shell (`mongosh`) Operations:**
  - `db.<collection>.insertOne({...})`: Inserts a single document and automatically generates a unique `_id` (`ObjectId`).
  - `db.<collection>.insertMany([{...}, ...])`: Atomically inserts a batch array of documents.
  - `db.<collection>.find()`: Retrieves documents matching a filter predicate.
  - `.pretty()`: Formats returned documents in readable JSON structure.
- **Advantages:** Horizontal scalability, schema agility, and high write throughput.

---

## 3. Code
```bash
# 1. Install MongoDB and MongoDB shell (mongosh)
!curl -fsSL https://www.mongodb.org/static/pgp/server-7.0.asc | gpg --yes --dearmor -o /usr/share/keyrings/mongodb-server-7.0.gpg
!echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-7.0.list
!apt-get update -qq && apt-get install -y -qq mongodb-org mongodb-mongosh > /dev/null

# 2. Start MongoDB Service in the background
!systemctl start mongod || service mongodb start || mongod --fork --logpath /var/log/mongodb.log --dbpath /var/lib/mongodb

# 3. Run the saved MongoDB JavaScript script using mongosh
!mongosh --quiet --eval 'use practical3' --file practical_03_mongodb.js
```

---

## 4. Output
```text
deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse
W: Skipping acquire of configured file 'main/source/Sources' as repository 'https://r2u.stat.illinois.edu/ubuntu noble InRelease' does not seem to provide it (sources.list entry misspelt?)
System has not been booted with systemd as init system (PID 1). Can't operate.
Failed to connect to bus: Host is down
mongodb: unrecognized service
about to fork child process, waiting until server is ready for connections.
forked process: 4304
child process started successfully, parent exiting
0;mongosh mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000switched to db practical3

## Step 1: Insert Single Document
Query: db.items.insertOne({ name: "laptop", price: 999 });
{
  acknowledged: true,
  insertedId: ObjectId('6ac904bbaa1299817862fab1')
}

## Step 2: View Inserted Document
Query: db.items.find();
[
  {
    _id: ObjectId('6ac904bbaa1299817862fab1'),
    name: 'laptop',
    price: 999
  }
]

## Step 3: Insert Multiple Documents
Query: db.products.insertMany([...3 products...]);
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('6ac904bbaa1299817862fab2'),
    '1': ObjectId('6ac904bbaa1299817862fab3'),
    '2': ObjectId('6ac904bbaa1299817862fab4')
  }
}

## Step 4: View Products in Formatted JSON
Query: db.products.find().pretty();
[
  {
    _id: ObjectId('6ac904bbaa1299817862fab2'),
    name: 'phone',
    price: 500,
    stock: 10
  },
  {
    _id: ObjectId('6ac904bbaa1299817862fab3'),
    name: 'tablet',
    price: 300,
    stock: 5
  },
  {
    _id: ObjectId('6ac904bbaa1299817862fab4'),
    name: 'watch',
    price: 150,
    stock: 0
  }
]

========================================
Submitted by: Wali Momin
========================================
0;
```

---

## 5. Conclusion
Successfully provisioned a MongoDB instance and executed document operations via `mongosh`. Demonstrated document insertion (`insertOne`, `insertMany`) and formatted retrieval (`find().pretty()`), validating NoSQL schema flexibility.
