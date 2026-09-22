"""
Profile the parquet file produced by the twelve_data extract step.

Usage:
    python scripts/profile_parquet.py /tmp/extracted_data.parquet

Or run the DAG, then immediately before the file is deleted:
    python scripts/profile_parquet.py
"""

import sys
import os
import pandas as pd

PARQUET_PATH = sys.argv[1] if len(sys.argv) > 1 else "/tmp/extracted_data.parquet"

if not os.path.exists(PARQUET_PATH):
    sys.exit(f"File not found: {PARQUET_PATH}  (run this before the load step deletes it)")

df = pd.read_parquet(PARQUET_PATH)

print("=" * 72)
print("1. SHAPE")
print("=" * 72)
print(f"  Rows:    {df.shape[0]:,}")
print(f"  Columns: {df.shape[1]}")
print()

print("=" * 72)
print("2. DTYPES & MEMORY")
print("=" * 72)
print(df.dtypes.to_string())
print()
mem = df.memory_usage(deep=True)
print(f"  Total memory:  {mem.sum() / 1024:.1f} KB")
print(f"  Per-column:")
for col, bytes_used in mem.items():
    print(f"    {col:<12}  {bytes_used / 1024:8.1f} KB")
print()

print("=" * 72)
print("3. NULL COUNTS & PERCENTAGE")
print("=" * 72)
nulls = df.isnull().sum()
null_pct = (nulls / len(df) * 100).round(2)
null_report = pd.DataFrame({"null_count": nulls, "null_pct": null_pct})
print(null_report.to_string())
print()

print("=" * 72)
print("4. DESCRIBE (numeric-like columns)")
print("=" * 72)
# TwelveData API returns numbers as strings; describe() works best after casting.
# Show raw describe first (object columns), then cast attempt.
print("-- Raw (object columns):")
print(df.describe(include="all").to_string())
print()

# Attempt numeric cast for price/volume columns
num_cols = ["open", "high", "low", "close", "volume"]
available_num_cols = [c for c in num_cols if c in df.columns]
if available_num_cols:
    df_numeric = df[available_num_cols].apply(pd.to_numeric, errors="coerce")
    print("-- After pd.to_numeric (errors='coerce'):")
    print(df_numeric.describe().to_string())
    coercion_nans = df_numeric.isnull().sum()
    original_nans = df[available_num_cols].isnull().sum()
    new_nans = coercion_nans - original_nans
    if new_nans.any():
        print()
        print("  ⚠  Columns that lost values during numeric coercion:")
        for col in available_num_cols:
            if new_nans[col] > 0:
                print(f"     {col}: {new_nans[col]} non-numeric values coerced to NaN")
                # Show the offending values
                mask = pd.to_numeric(df[col], errors="coerce").isna() & df[col].notna()
                bad_vals = df.loc[mask, col].unique()[:10]
                print(f"       Examples: {list(bad_vals)}")
print()

print("=" * 72)
print("5. VALUE COUNTS ON KEY COLUMNS")
print("=" * 72)
for col in ["symbol", "interval", "currency", "exchange"]:
    if col in df.columns:
        print(f"\n-- {col} ({df[col].nunique()} unique values):")
        print(df[col].value_counts().to_string())
print()

print("=" * 72)
print("6. DUPLICATE ROW CHECK")
print("=" * 72)
dup_cols = ["symbol", "datetime"] if "symbol" in df.columns and "datetime" in df.columns else df.columns.tolist()
dups = df.duplicated(subset=dup_cols, keep=False)
print(f"  Duplicate (symbol + datetime) rows: {dups.sum():,}")
if dups.any():
    print("  Sample duplicates:")
    print(df[dups].sort_values(dup_cols).head(10).to_string())
print()

print("=" * 72)
print("7. DATETIME RANGE PER SYMBOL")
print("=" * 72)
if "symbol" in df.columns and "datetime" in df.columns:
    grouped = df.groupby("symbol")["datetime"].agg(["min", "max", "count"])
    print(grouped.to_string())
print()

print("=" * 72)
print("8. SAMPLE ROWS (head 5)")
print("=" * 72)
print(df.head().to_string())
print()

print("=" * 72)
print("9. TYPE RECOMMENDATIONS")
print("=" * 72)
recommendations = []
if "datetime" in df.columns:
    recommendations.append("  datetime → cast to pd.to_datetime() before loading to Snowflake")
for col in available_num_cols:
    recommendations.append(f"  {col} → cast to float (pd.to_numeric) — currently stored as string")
if recommendations:
    print("\n".join(recommendations))
else:
    print("  (no recommendations)")
print()
print("DONE.")
