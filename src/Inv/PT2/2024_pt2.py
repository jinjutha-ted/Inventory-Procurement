#%%
import polars as pl
import sys
import time

# Add parent directory to path for module imports
sys.path.append('../../')

# Standard imports
import pandas as pd
import chardet
import glob
import os
import json
from function.import_data import *
from function.parseThaiDate import *
from function.exportExcel import *

start_time = time.time()

# --------------------------------------------------------------------
# 0 Load configuration
# --------------------------------------------------------------------
config_path = "../../config.json"
with open(config_path, 'r') as file:
    config = json.load(file)

#%%
# --------------------------------------------------------------------
# 1 Load and combine all PT2 INV_VALUE Excel files (2024 only)
# --------------------------------------------------------------------
inv_value_pt2_folder = "../../../../Data/Result/clean_data/Inventory/2024/PT2/INV_VALUE"
output_inv_value_pt2_parquet_path = "../../../../Data/Result/Combined/INV_VALUE/pt2_combined.parquet"

# Define columns to extract from INV_VALUE files
required_inv_columns = [
    "SubInventory",
    "SubInventory Description", 
    "Item", 
    "Item Description",
    "Category",
    "UOM", 
    "UOM Name", 
    "Quantity", 
    "Unit Cost", 
    "Extended Value"
]

# Get list of INV_VALUE Excel files
excel_files = glob.glob(os.path.join(inv_value_pt2_folder, "*.xlsx"))

df_list = []

for file in excel_files:
    try:
        # Extract month number from filename (e.g. 'APR2024' -> 4)
        filename = os.path.basename(file)
        month_str = filename.split('by_')[-1].split('.')[0][:3]
        month_int = time.strptime(month_str, '%b').tm_mon

        # Load and select required columns
        df = pl.read_excel(file)
        df = df.select([col for col in required_inv_columns if col in df.columns])

        # Add [End of Month] column
        df = df.with_columns([
            pl.lit(month_int).alias("End of Month")
        ])

        df_list.append(df)

    except Exception as e:
        print(f"❌ Error reading {file}: {e}")

# Combine all INV_VALUE data
combined_df = pl.concat(df_list)

# Save combined INV_VALUE data to Parquet
combined_df.write_parquet(output_inv_value_pt2_parquet_path)

#%%

# --------------------------------------------------------------------
# 2 Load and filter all DOS_ITEM Excel files (for item mapping)
# --------------------------------------------------------------------
dos_item_folder = "../../../../Data/Result/clean_data/Inventory/2024/PT2/DOS_ITEM"
output_dos_item_parquet_path = "../../../../Data/Result/Combined/INV_VALUE/pt2_combined.parquet"

# Define columns to extract from DOS_ITEM files
required_dos_columns = [
    "CREATION_DATE",
    "ITEM_CATEGORY", "ITEM_CATEGORY_DESC",
    "ITEM_NUMBER", "ITEM_DESCRIPTION",
    "DOS_GROUP", "DOS_GROUP_DESC",
    "PRIMARY_UOM_CODE", "PRIMARY_UNIT_OF_MEASURE",
    "Local / Import", "Generic / Original"
]

dos_item_df_list = []

for file in glob.glob(os.path.join(dos_item_folder, "*.xlsx")):
    try:
        df = pl.read_excel(file)
        df = df.select([col for col in required_dos_columns if col in df.columns])

        # ✅ Fix: only check for null, skip string comparison
        df = df.filter(pl.col("CREATION_DATE").is_not_null())

        # Cast everything else to string, except CREATION_DATE
        df = df.with_columns([
            pl.col(col).cast(pl.Utf8) for col in df.columns if col != "CREATION_DATE"
        ])

        dos_item_df_list.append(df)

    except Exception as e:
        print(f"❌ Error reading DOS_ITEM file {file}: {e}")


dos_item_df = pl.concat(dos_item_df_list).unique()



# --------------------------------------------------------------------
# 3 Compare and mark matches between combined_df and dos_item_df
# --------------------------------------------------------------------
dos_item_df = dos_item_df.with_columns([
    (pl.col("ITEM_NUMBER") + pl.col("ITEM_DESCRIPTION")).alias("item_key")
])

combined_df = combined_df.with_columns([
    (pl.col("Item") + pl.col("Item Description")).alias("item_key")
])

combined_df = combined_df.with_columns([
    pl.col("item_key").is_in(dos_item_df["item_key"]).cast(pl.Int8).alias("In DOS_ITEM")
])

# --------------------------------------------------------------------
# 4 Save result
# --------------------------------------------------------------------
combined_df.write_parquet(output_dos_item_parquet_path)
print(f"✅ Done! Execution time: {(time.time() - start_time):.2f} seconds")

# %%

# --------------------------------------------------------------------
# 5 combine all sale excel file
# --------------------------------------------------------------------

output_sale_parquet_path = "../../../../Data/Result/Combined/DOS_SALE/pt2_sale_combined.parquet"
output_orcmii_parquet_path = "../../../../Data/Result/Combined/ORCMII/pt2_orcmii_combined.parquet"
output_posmis_parquet_path = "../../../../Data/Result/Combined/POSMIS/pt2_posmis_combined.parquet"
output_ssbmic_parquet_path = "../../../../Data/Result/Combined/SSBMIC/pt2_ssbmic_combined.parquet"
output_hismic_parquet_path = "../../../../Data/Result/Combined/HISMIC/pt2_hismic_combined.parquet"

import polars as pl
import glob
import os

# Paths
dos_sale_folder = "../../../../Data/Result/clean_data/Inventory/2024/PT2/DOS_SALE"
orcmii_folder = "../../../../Data/Result/clean_data/Inventory/2024/PT2/ORCMII"
posmis_folder = "../../../../Data/Result/clean_data/Inventory/2024/PT2/POSMIS"
ssbmic_folder = "../../../../Data/Result/clean_data/Inventory/2024/PT2/SSBMIC"
hismic_folder = "../../../../Data/Result/clean_data/Inventory/2024/PT2/HISMIC"

# Columns
required_dos_sale_columns = [
    "SUBINVENTORY_CODE", "TRANSACTION_DATE", "ITEM_CODE", "ITEM_DESC",
    "TRX_TYPE_NAME", "TRX_TYPE_DESC", "PRIMARY_UOM_CODE", "PRIMARY_UOM_NAME"
]

required_other_columns = [
    "Item", "Subinventory", "Transaction Date",
    "Transaction ID", "Transaction UOM", "Primary Quantity"
]

def load_and_combine(folder, required_cols, output_parquet_path):
    df_list = []
    for file in glob.glob(os.path.join(folder, "*.xlsx")):
        try:
            df = pl.read_excel(file)
            df = df.select([col for col in required_cols if col in df.columns])
            df_list.append(df)
        except Exception as e:
            print(f"❌ Error reading file {file}: {e}")
    
    # Combine and deduplicate
    df_combined = pl.concat(df_list).unique() if df_list else pl.DataFrame()
    
    # Save to parquet
    df_combined.write_parquet(output_parquet_path)
    
    return df_combined

# Load files
dos_sale_df = load_and_combine(dos_sale_folder, required_dos_sale_columns, output_sale_parquet_path)
orcmii_df = load_and_combine(orcmii_folder, required_other_columns, output_orcmii_parquet_path)
posmis_df = load_and_combine(posmis_folder, required_other_columns, output_posmis_parquet_path)
ssbmic_df = load_and_combine(ssbmic_folder, required_other_columns, output_ssbmic_parquet_path)
hismic_df = load_and_combine(hismic_folder, required_other_columns, output_hismic_parquet_path)

# 1️⃣ Combine non-empty sources using only "Item" column
source_dfs = [orcmii_df, posmis_df, ssbmic_df, hismic_df]

def extract_item_column(df):
    if df.shape[0] > 0 and "Item" in df.columns:
        return df.select([
            pl.col("Item").cast(pl.Utf8)
        ])
    return None

# Apply and filter out None
item_only_sources = [extract_item_column(df) for df in source_dfs]
item_only_sources = [df for df in item_only_sources if df is not None]

# Concatenate and drop duplicates
combined_sources = pl.concat(item_only_sources).unique() if item_only_sources else pl.DataFrame()

# 2️⃣ Prepare comparison key: ITEM_CODE from sale, Item from sources
dos_sale_df = dos_sale_df.with_columns([
    pl.col("ITEM_CODE").alias("item_key")
])
combined_sources = combined_sources.with_columns([
    pl.col("Item").alias("item_key")
])

# 2️⃣ Mark: whether each item in dos_sale_df exists in the 4 combined source files
dos_sale_df = dos_sale_df.with_columns([
    pl.col("item_key").is_in(combined_sources["item_key"]).cast(pl.Int8).alias("Exists in 4 Source Files")
])

dos_sale_df.write_parquet(output_sale_parquet_path)

#%%

# Output paths
output_sale_parquet_path = "../../../../Data/Result/Combined/DOS_SALE/pt2_sale_combined.parquet"
output_orcmii_parquet_path = "../../../../Data/Result/Combined/ORCMII/pt2_orcmii_combined.parquet"
output_posmis_parquet_path = "../../../../Data/Result/Combined/POSMIS/pt2_posmis_combined.parquet"
output_ssbmic_parquet_path = "../../../../Data/Result/Combined/SSBMIC/pt2_ssbmic_combined.parquet"
output_hismic_parquet_path = "../../../../Data/Result/Combined/HISMIC/pt2_hismic_combined.parquet"

# Reload combined data
dos_sale_df = pl.read_parquet(output_sale_parquet_path)
orcmii_df = pl.read_parquet(output_orcmii_parquet_path)
posmis_df = pl.read_parquet(output_posmis_parquet_path)
ssbmic_df = pl.read_parquet(output_ssbmic_parquet_path)
hismic_df = pl.read_parquet(output_hismic_parquet_path)

# Prepare item_key in DOS_SALE
dos_sale_df = dos_sale_df.with_columns([
    pl.col("ITEM_CODE").cast(pl.Utf8).alias("item_key")
])

# Function to add check column to each source df
def mark_exists_in_sale(df, source_name):
    if df.shape[0] > 0 and "Item" in df.columns:
        df = df.with_columns([
            pl.col("Item").cast(pl.Utf8).alias("item_key")
        ])
        df = df.with_columns([
            pl.col("item_key").is_in(dos_sale_df["item_key"]).cast(pl.Int8).alias("Exists in DOS Sale Files")
        ])
    return df

# Apply to each source
orcmii_df = mark_exists_in_sale(orcmii_df, "ORCMII")
posmis_df = mark_exists_in_sale(posmis_df, "POSMIC")
ssbmic_df = mark_exists_in_sale(ssbmic_df, "SSBMIC")
hismic_df = mark_exists_in_sale(hismic_df, "HISMIC")

# Save updated sources
orcmii_df.write_parquet(output_orcmii_parquet_path)
posmis_df.write_parquet(output_posmis_parquet_path)
ssbmic_df.write_parquet(output_ssbmic_parquet_path)
hismic_df.write_parquet(output_hismic_parquet_path)

#%%

output_inv_value_pt2_parquet_path = "../../../../Data/Result/Combined/INV_VALUE/pt2_combined.parquet"
pt2_combined = load_parquet(output_inv_value_pt2_parquet_path)

# 1️⃣ Create item_key in dos_sale_df
dos_sale_df = dos_sale_df.with_columns([
    (pl.col("ITEM_CODE").cast(pl.Utf8) + pl.col("ITEM_DESC").cast(pl.Utf8)).alias("item_key")
])


# 3️⃣ Check if each combined_df item_key exists in dos_sale_df
pt2_combined = pt2_combined.with_columns([
    pl.col("item_key").is_in(dos_sale_df["item_key"]).cast(pl.Int8).alias("Exists in DOS Sale Files")
])

# 4️⃣ Save result
pt2_combined.write_parquet(output_inv_value_pt2_parquet_path)

# %%
