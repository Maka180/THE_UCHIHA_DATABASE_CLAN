import pandas as pd
import os

# Folder containing your Excel files
folder_path = r"C:\Users\USER\Desktop\CMPG321ROJECT\THE_UCHIHA_DATABASE_CLAN\Other_files"

# List of files to clean
file_list = [
    "Trans Types (1).xlsx",
    "Suppliers (1).xlsx",
    "Age Analysis (1).xlsx"
]

for file_name in file_list:
    file_path = os.path.join(folder_path, file_name)
    
    # Load Excel file
    df = pd.read_excel(file_path)
    print(f"\n🔹 Original Data Preview for {file_name}:")
    print(df.head(5))

    # --- Cleaning steps ---
    df = df.dropna(how="all")                # drop fully empty rows
    df = df.drop_duplicates()                # remove duplicates
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)  # trim strings
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")  # standardize column names
    df.reset_index(drop=True, inplace=True)  # reset index

    # --- Save cleaned data ---
    cleaned_file_xlsx = os.path.join(folder_path, f"Cleaned_{file_name}")
    df.to_excel(cleaned_file_xlsx, index=False)

    cleaned_file_csv = cleaned_file_xlsx.replace(".xlsx", ".csv")
    df.to_csv(cleaned_file_csv, index=False)

    cleaned_file_json = cleaned_file_xlsx.replace(".xlsx", ".json")
    df.to_json(cleaned_file_json, orient="records", indent=4)

    print(f"✅ Cleaned {file_name} saved as:")
    print(f"   - Excel: {cleaned_file_xlsx}")
    print(f"   - CSV:   {cleaned_file_csv}")
    print(f"   - JSON:  {cleaned_file_json}")