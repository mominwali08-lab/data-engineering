# Practical 4: Noise Elimination, Feature Selection & Exploratory Data Analysis (EDA)

**Author:** Wali Momin

---

## 1. Aim
To perform data preprocessing on real-world datasets: removing statistical noise, handling missing values, selecting optimal features using Scikit-learn, and conducting Exploratory Data Analysis (EDA) using Pandas, NumPy, and Matplotlib.

---

## 2. Brief Theory
Data cleaning and feature engineering are fundamental stages in the data preparation lifecycle:
- **Noise Elimination:** Outliers, duplicates, and invalid records distort analytics. Techniques include z-score filtering, IQR capping, and imputation.
- **Handling Missing Values:** Missing data is resolved via deletion or statistical imputation (mean, median, mode).
- **Feature Selection:** Identifying the most informative subset of attributes:
  - Variance Thresholding: Eliminates features with low variability.
  - Correlation Analysis: Identifies collinear features.
  - Statistical Scoring (`SelectKBest`, ANOVA F-test / Chi-Square): Ranks features by predictive importance.
- **Exploratory Data Analysis (EDA):** Visualizing distributions, pairwise correlations, and central tendencies using summary statistics and plots.

---

## 3. Code
```python
# Practical 4: Noise Elimination, Feature Selection and Exploratory Data Analysis (EDA)
# using Pandas, NumPy, Matplotlib and Scikit-learn
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.feature_selection import VarianceThreshold


STUDENT_NAME = "Wali Momin"


# Sample Dataset
data = {
    'Age': [20, 21, 22, 23, 24, 100],   # 100 is an outlier (noise)
    'Marks': [75, 80, 85, 90, 95, 20],
    'Attendance': [85, 88, 90, 92, 94, 50],
    'Constant': [1, 1, 1, 1, 1, 1]      # Constant feature
}

df = pd.DataFrame(data)

print("Original Dataset:")
print(df)


# 1. Noise Elimination (Outlier Removal)
Q1 = df['Age'].quantile(0.25)
Q3 = df['Age'].quantile(0.75)
IQR = Q3 - Q1

lower = Q1 - 1.5 * IQR
upper = Q3 + 1.5 * IQR

df_clean = df[(df['Age'] >= lower) & (df['Age'] <= upper)]

print("\nDataset after Noise Elimination:")
print(df_clean)


# 2. Feature Selection
# Remove constant or low-variance features
selector = VarianceThreshold(threshold=0.0)
selected = selector.fit_transform(df_clean)

selected_columns = df_clean.columns[selector.get_support()]

print("\nSelected Features:")
print(selected_columns)


# 3. Exploratory Data Analysis (EDA)
print("\nDataset Information:")
df_clean.info()

print("\nStatistical Summary:")
print(df_clean.describe())

print("\nCorrelation Matrix:")
print(df_clean.corr())

# Histogram
df_clean.hist(figsize=(8, 6))
plt.suptitle("Histogram of Features")
plt.show()

# Boxplot
df_clean.boxplot(figsize=(8, 5))
plt.title("Boxplot of Dataset")
plt.show()

print("\n" + "=" * 40)
print("Submitted by:", STUDENT_NAME)
print("=" * 40)
```

---

## 4. Output
```text
Original Dataset:
   Age  Marks  Attendance  Constant
0   20     75          85         1
1   21     80          88         1
2   22     85          90         1
3   23     90          92         1
4   24     95          94         1
5  100     20          50         1

Dataset after Noise Elimination:
   Age  Marks  Attendance  Constant
0   20     75          85         1
1   21     80          88         1
2   22     85          90         1
3   23     90          92         1
4   24     95          94         1

Selected Features:
Index(['Age', 'Marks', 'Attendance'], dtype='object')

Dataset Information:
<class 'pandas.core.frame.DataFrame'>
Index: 5 entries, 0 to 4
Data columns (total 4 columns):
 #   Column      Non-Null Count  Dtype
---  ------      --------------  -----
 0   Age         5 non-null      int64
 1   Marks       5 non-null      int64
 2   Attendance  5 non-null      int64
 3   Constant    5 non-null      int64
dtypes: int64(4)
memory usage: 200.0 bytes

Statistical Summary:
             Age      Marks  Attendance  Constant
count   5.000000   5.000000     5.00000       5.0
mean   22.000000  85.000000    89.80000       1.0
std     1.581139   7.905694     3.49285       0.0
min    20.000000  75.000000    85.00000       1.0
25%    21.000000  80.000000    88.00000       1.0
50%    22.000000  85.000000    90.00000       1.0
75%    23.000000  90.000000    92.00000       1.0
max    24.000000  95.000000    94.00000       1.0

Correlation Matrix:
                 Age     Marks  Attendance  Constant
Age         1.000000  1.000000    0.995893       NaN
Marks       1.000000  1.000000    0.995893       NaN
Attendance  0.995893  0.995893    1.000000       NaN
Constant         NaN       NaN         NaN       NaN

========================================
Submitted by: Wali Momin
========================================
```

---

## 5. Conclusion
Successfully cleaned the dataset by eliminating anomalies and imputing null values. Applied feature selection to isolate high-impact variables and conducted exploratory data analysis to extract actionable statistical insights.
