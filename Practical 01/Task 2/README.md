# Practical 1 - Task 2: Reading and Writing Binary Files

**Author:** Wali Momin

---

## 1. Aim
To serialize structured data into compact binary representations, write them to disk, and accurately deserialize and unpack binary data using Python's `struct` module.

---

## 2. Brief Theory
Binary file handling allows data engineers to store numerical and fixed-length data in a highly compact, non-human-readable format that provides significant performance advantages over plain text formats:
- **`struct` module:** Converts between Python values and C structs represented as Python `bytes` objects.
- **Format Strings:**
  - `<`: Standard size, little-endian byte ordering.
  - `i`: 4-byte integer.
  - `10s`: 10-byte string (padded with null bytes).
  - `f`: 4-byte single-precision float.
- **`struct.pack(fmt, v1, v2, ...)`:** Packs Python objects into contiguous bytes according to the format string.
- **`struct.unpack(fmt, buffer)`:** Reconstructs original Python data types from binary byte streams.

---

## 3. Code
```python
"""
Practical 1 - Exercise 2
Read and write binary files.
Standard library only. Runs locally and on Google Colab.
"""
import os
import struct


STUDENT_NAME = "Wali Momin"

# EXERCISE 2
RECORD = struct.Struct("<i10sf")


def exercise2():
    print("\n" + "#" * 60 + "\n# EXERCISE 2: Reading and writing binary files\n" + "#" * 60)
    students = [(1, "Asha", 88.5), (2, "Ravi", 72.0), (3, "Meena", 95.25)]


    with open("students.bin", "wb") as f:
        for sid, name, marks in students:
            f.write(RECORD.pack(sid, name.encode().ljust(10, b"\0"), marks))
    print("Written students.bin:", os.path.getsize("students.bin"), "bytes",
          f"({RECORD.size} bytes per record)")


    print("\nReading all records:")
    with open("students.bin", "rb") as f:
        while chunk := f.read(RECORD.size):
            sid, name, marks = RECORD.unpack(chunk)
            print(" ", sid, name.rstrip(b"\0").decode(), marks)


    with open("students.bin", "ab") as f:
        f.write(RECORD.pack(4, b"John".ljust(10, b"\0"), 61.5))
    print("\nAfter append, records in file:", os.path.getsize("students.bin") // RECORD.size)


    with open("students.bin", "rb") as f:
        f.seek(2 * RECORD.size)
        sid, name, marks = RECORD.unpack(f.read(RECORD.size))
        print("\nRandom access (record 3):", sid, name.rstrip(b"\0").decode(), marks)


    with open("students.bin", "r+b") as f:
        f.seek(RECORD.size)
        sid, name, _ = RECORD.unpack(f.read(RECORD.size))
        f.seek(RECORD.size)
        f.write(RECORD.pack(sid, name, 80.0))
    with open("students.bin", "rb") as f:
        f.seek(RECORD.size)
        print("After update:", RECORD.unpack(f.read(RECORD.size)))


    data = bytes([72, 101, 108, 108, 111, 0, 255, 16])
    with open("raw.bin", "wb") as f:
        f.write(data)
    with open("raw.bin", "rb") as src_f, open("raw_copy.bin", "wb") as dst_f:
        while block := src_f.read(4):
            dst_f.write(block)
    with open("raw_copy.bin", "rb") as f:
        content = f.read()
    print("\nRaw bytes :", list(content))
    print("Hex dump  :", content.hex(" "))
    print("Copy identical to original:", content == data)


    size = os.path.getsize("students.bin")
    print("File size valid:", size % RECORD.size == 0)


if __name__ == "__main__":
    exercise2()
    print("\n" + "=" * 40)
    print("Submitted by:", STUDENT_NAME)
    print("=" * 40)
```

---

## 4. Output
```text
############################################################
# EXERCISE 2: Reading and writing binary files
############################################################
Written students.bin: 54 bytes (18 bytes per record)

Reading all records:
  1 Asha 88.5
  2 Ravi 72.0
  3 Meena 95.25

After append, records in file: 4

Random access (record 3): 3 Meena 95.25
After update: (2, b'Ravi\x00\x00\x00\x00\x00\x00', 80.0)

Raw bytes : [72, 101, 108, 108, 111, 0, 255, 16]
Hex dump  : 48 65 6c 6c 6f 00 ff 10
Copy identical to original: True
File size valid: True

========================================
Submitted by: Wali Momin
========================================
```

---

## 5. Conclusion
Successfully demonstrated high-performance binary serialization and deserialization in Python. Confirmed that binary encoding significantly minimizes file footprint (54 bytes for structured student records) and retains data precision.
