import pandas as pd

FILE = "sic_codes.xlsx"

xls = pd.ExcelFile(FILE)

print("="*80)
print("SHEETS")
print("="*80)

for sheet in xls.sheet_names:
    print(sheet)

print()

for sheet in xls.sheet_names:

    print("="*80)
    print(sheet)
    print("="*80)

    df = pd.read_excel(FILE, sheet_name=sheet)

    print("\nShape:", df.shape)

    print("\nColumns:")

    for c in df.columns:
        print(" -", c)

    print("\nFirst rows:")

    print(df.head(10))

    print("\n")
