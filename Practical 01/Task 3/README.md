# Practical 1 - Task 3: Pattern Matching & String Processing with Regular Expressions

**Author:** Wali Momin

---

## 1. Aim
To search, split, validate, and replace strings using Regular Expressions (`re` module) in Python for data cleaning and extraction.

---

## 2. Brief Theory
Regular expressions provide declarative pattern-matching mechanisms essential for text extraction, format validation, and data wrangling:
- **`re.search(pattern, string)`:** Scans through a string to find the first location where the regex matches.
- **`re.findall(pattern, string)`:** Returns all non-overlapping matches as a list of strings or tuples.
- **`re.finditer(pattern, string)`:** Returns an iterator yielding `Match` objects for all matches.
- **`re.split(pattern, string)`:** Splits a string by occurrences of the pattern (e.g. multi-character delimiters).
- **`re.sub(pattern, repl, string)`:** Replaces matched patterns with specified replacement strings or dynamic callback values.
- Common regex anchors and tokens: `\d` (digit), `\w` (word character), `[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}` (email pattern).

---

## 3. Code
```python
"""
Practical 1 - Exercise 3
Search, split and replace strings using regular expressions.
Standard library only. Runs locally and on Google Colab.
"""
import re


STUDENT_NAME = "Wali Momin"

# EXERCISE 3
def exercise3():
    print("\n" + "#" * 60 + "\n# EXERCISE 3: Regular expressions\n" + "#" * 60)
    text = ("Contact Asha at asha@mail.com or 98765-43210. "
            "Ravi: ravi_k@college.edu.in, phone 91234-56789. "
            "Meeting on 12/03/2025 and 2025-04-18. Invalid mail: bob@@x")

    # SEARCH
    m = re.search(r"\d{5}-\d{5}", text)
    print("First phone (search):", m.group(), "at", m.span())
    print("Emails (findall)    :", re.findall(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+", text))
    print("Phones (findall)    :", re.findall(r"\b\d{5}-\d{5}\b", text))
    print("Dates (finditer)    :", [(d.group(), d.start()) for d in
                                    re.finditer(r"\d{2}/\d{2}/\d{4}|\d{4}-\d{2}-\d{2}", text)])

    # groups and named groups
    d = re.search(r"(?P<day>\d{2})/(?P<month>\d{2})/(?P<year>\d{4})", text)
    print("Named groups        :", d.groupdict())
    print("match vs fullmatch  :", bool(re.match(r"Contact", text)),
          bool(re.fullmatch(r"\d+", "12345")))

    # SPLIT
    print("\nSplit on , ; or spaces:", re.split(r"[,; ]+", "a,b; c  d,e"))
    print("Split, keep delimiters :", re.split(r"([,;])", "a,b;c"))
    print("Split on sentences     :", re.split(r"(?<=\.)\s+", text)[:2])

    # REPLACE
    print("\nMask phones   :", re.sub(r"\b(\d{5})-(\d{5})\b", r"XXXXX-\2", text)[:60], "...")
    print("Reformat date :", re.sub(r"(\d{2})/(\d{2})/(\d{4})", r"\3-\2-\1", "12/03/2025"))
    print("Collapse space:", re.sub(r"\s+", " ", "too    many     spaces"))
    print("Function repl :", re.sub(r"\d+", lambda x: str(int(x.group()) * 2), "a1 b22 c3"))
    print("subn (count)  :", re.subn(r"a", "A", "banana"))

    # greedy vs non-greedy, flags (IGNORECASE, MULTILINE)
    html = "<b>bold</b> and <i>italic</i>"
    print("\nGreedy      :", re.findall(r"<.+>", html))
    print("Non-greedy  :", re.findall(r"<.+?>", html))
    print("IGNORECASE  :", re.findall(r"asha", text, re.IGNORECASE))
    lines = "first line\nsecond line\nthird line"
    print("MULTILINE ^ :", re.findall(r"^\w+", lines, re.MULTILINE))
    print("Replace count=1:", re.sub(r"line", "LINE", lines, count=1))

    # compiled pattern with flags + validation
    pat = re.compile(r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$", re.IGNORECASE)
    for e in ["Asha@Mail.com", "bob@@x", "no-at-sign.com", "ok@site.org"]:
        print(f"Valid email? {e:18} ->", bool(pat.match(e)))


if __name__ == "__main__":
    exercise3()
    print("\n" + "=" * 40)
    print("Submitted by:", STUDENT_NAME)
    print("=" * 40)
```

---

## 4. Output
```text
############################################################
# EXERCISE 3: Regular expressions
############################################################
First phone (search): 98765-43210 at (33, 44)
Emails (findall)    : ['asha@mail.com', 'ravi_k@college.edu.in']
Phones (findall)    : ['98765-43210', '91234-56789']
Dates (finditer)    : [('12/03/2025', 105), ('2025-04-18', 120)]
Named groups        : {'day': '12', 'month': '03', 'year': '2025'}
match vs fullmatch  : True True

Split on , ; or spaces: ['a', 'b', 'c', 'd', 'e']
Split, keep delimiters : ['a', ',', 'b', ';', 'c']
Split on sentences     : ['Contact Asha at asha@mail.com or 98765-43210.', 'Ravi: ravi_k@college.edu.in, phone 91234-56789.']

Mask phones   : Contact Asha at asha@mail.com or XXXXX-43210. Ravi: ravi_k@c ...
Reformat date : 2025-03-12
Collapse space: too many spaces
Function repl : a2 b44 c6
subn (count)  : ('bAnAnA', 3)

Greedy      : ['<b>bold</b> and <i>italic</i>']
Non-greedy  : ['<b>', '</b>', '<i>', '</i>']
IGNORECASE  : ['Asha', 'asha']
MULTILINE ^ : ['first', 'second', 'third']
Replace count=1: first LINE
second line
third line
Valid email? Asha@Mail.com      -> True
Valid email? bob@@x             -> False
Valid email? no-at-sign.com     -> False
Valid email? ok@site.org        -> True

========================================
Submitted by: Wali Momin
========================================
```

---

## 5. Conclusion
Successfully applied regular expressions to validate structured text formats (phone numbers, email addresses, dates) and sanitize textual data. Demonstrated the efficiency of regex operations for unstructured log analysis and text parsing.
