import pandas as pd

# Load the Excel file
file_path = r"C:\Users\Neo\Desktop\2025\cmpg321\ClearVue Project\Customer Categories.xlsx"  # make sure the file is in your project folder
df = pd.read_excel(file_path)

print("🔹 Original Data Preview:")
print(df.head(10))

# 1. Trim whitespace from all string columns
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# 2. Standardize column names (remove spaces, make lowercase)
df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

# 3. Handle missing values
df = df.dropna(how="all")   # drop fully empty rows
df = df.drop_duplicates()   # remove duplicate rows

# 4. Try to convert numeric columns
for col in df.columns:
    df[col] = pd.to_numeric(df[col], errors="ignore")

# 5. Reset index
df = df.reset_index(drop=True)

print("\n✅ Cleaned Data Preview:")
print(df.head(10))

# Save cleaned data
df.to_csv("cleaned_CustomerCategories.csv", index=False)
df.to_json("cleaned_CustomerCategories.json", orient="records", indent=4)

print("\nCleaning complete! Files saved as 'cleaned_CustomerCategories.csv' and 'cleaned_CustomerCategories.json'")