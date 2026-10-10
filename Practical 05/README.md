# Practical 5: Data Extraction from REST APIs and Flat Files

**Author:** Wali Momin

---

## 1. Aim
To extract live semi-structured data from public REST APIs using HTTP requests, ingest tabular flat files (CSV), clean and harmonize both streams, and merge them into a unified dataset using Python.

---

## 2. Brief Theory
Real-world data pipelines regularly extract data from heterogeneous endpoints:
- **REST APIs (Representational State Transfer):** Web services communicating over HTTP:
  - Methods: `GET` (read data), `POST` (send data).
  - Data Serialization: Typically returns JSON payloads.
  - Python `requests` / `urllib`: Dispatches requests and parses responses with error handling (status codes 200, 404, 500).
- **Flat File Extraction:** Fixed-delimiter text files (CSV, TSV) ingested via `pandas.read_csv()`.
- **Data Harmonization & Joining:** Reconciling schema differences, data type casting, deduplicating keys, and performing relational merges (`pd.merge`) on matching foreign identifiers.

---

## 3. Code
```python
# Practical 5: Extracting Data from APIs and Flat Files Using Python
# Objective: fetch user data from a public REST API, clean it, and merge it
#            with a flat CSV file containing location details.
import pandas as pd
import requests


STUDENT_NAME = "Wali Momin"


def extract_api_data(url):
    """Fetches data from a REST API and returns a DataFrame."""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        # Normalizing JSON data into a flat table
        return pd.json_normalize(data)
    except requests.exceptions.RequestException as e:
        print(f"API Error: {e}")
        return pd.DataFrame()


def extract_csv_data(file_path):
    """Reads a flat CSV file."""
    try:
        return pd.read_csv(file_path)
    except FileNotFoundError as e:
        print(f"File Error: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    # 1. API Extraction
    api_url = "https://jsonplaceholder.typicode.com/users"
    api_df = extract_api_data(api_url)
    if not api_df.empty:
        api_df = api_df[["id", "name", "email", "company.name"]]
        api_df.rename(columns={"company.name": "company"}, inplace=True)

    # 2. Flat File Extraction (Mock CSV Data)
    # 'locations.csv' has columns: id, city
    csv_file = "locations.csv"

    # Creating a dummy file for the lab test run
    pd.DataFrame(
        {
            "id": list(range(1, 11)),
            "city": [
                "New York", "London", "Paris", "Tokyo", "Berlin",
                "Delhi", "Sydney", "Moscow", "Cairo", "Beijing",
            ],
        }
    ).to_csv(csv_file, index=False)

    csv_df = extract_csv_data(csv_file)

    # 3. Data Transformation & Merging
    if not api_df.empty and not csv_df.empty:
        merged_df = pd.merge(api_df, csv_df, on="id", how="inner")
        print("\n--- Merged ETL Pipeline Data View ---")
        print(merged_df.head())

        # Save to target Data Lake / Warehouse stage
        merged_df.to_csv("cleaned_warehouse_profiles.csv", index=False)
        print("\nData successfully saved to 'cleaned_warehouse_profiles.csv'")

        # Check for missing values after the merge
        print("\nMissing values per column:\n", merged_df.isnull().sum())

    print("\n" + "=" * 40)
    print("Submitted by:", STUDENT_NAME)
    print("=" * 40)
```

---

## 4. Output
```text
--- Merged ETL Pipeline Data View ---
   id              name                      email             company  \
0   1     Leanne Graham          Sincere@april.biz     Romaguera-Crona   
1   2      Ervin Howell          Shanna@melissa.tv        Deckow-Crist   
2   3  Clementine Bauch         Nathan@yesenia.net  Romaguera-Jacobson   
3   4  Patricia Lebsack  Julianne.OConner@kory.org       Robel-Corkery   
4   5  Chelsey Dietrich   Lucio_Hettinger@annie.ca         Keebler LLC   

       city  
0  New York  
1    London  
2     Paris  
3     Tokyo  
4    Berlin  

Data successfully saved to 'cleaned_warehouse_profiles.csv'

Missing values per column:
 id         0
name       0
email      0
company    0
city       0
dtype: int64

========================================
Submitted by: Wali Momin
========================================
```

---

## 5. Conclusion
Successfully extracted real-time data from an external REST API and integrated it with local flat-file datasets. Performed schema alignment and data merging in Pandas, producing a clean, consolidated dataset ready for analytical modeling.
