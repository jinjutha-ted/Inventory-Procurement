#%%
import sys
import time
# Adding the parent directory to sys.path for module imports
sys.path.append('../')

import pandas as pd
import json
from function.import_data import *
from function.parseThaiDate import *
from function.exportExcel import *
from function.combine import *

start_time = time.time()

# Load configuration
config_path = "/Users/jinjuthatedcharoen/Documents/PPG/P'Aim/Inventory/Python/src/config.json"
with open(config_path, 'r') as file:
    config = json.load(file)

'''
    1) Combine INV_VALUE
    
        pt2_INV_VALUE_folder_path = "Data/Result/clean_data/Inventory/2024/PT2/INV_VALUE"

        combine all .xlsx file in pt2_INV_VALUE_folder_path in one df 
        0. Select custom needed columns
        1. recheck if column name are align or not if not we gonna fix it
        2. recheck data type if not we gonna fix it
        3. combine all and put in one df
        4. And export this df into a parquet file that save it this output path "Data/Result/clean_data/Combined"

'''

if __name__ == "__main__":
    
    inv_result_root = config["INV_CLEAN_ROOT"]  # Root folder for Inventory clean data
    
    YEAR_TO_PROCESS = ["2024"]
    # YEAR_TO_PROCESS = ["2024", "2025"]
    BU_TO_PROCESS = ["PLC"]
    # BU_TO_PROCESS = ["PT1", "PT2", "PT3", "PTP", "PLR", 
    #                  "PLC", "PLK", "PLS", "PTN", "PTS", "PS2"]
    
    output_folder = config["combined_folder_path"]
    
    # Specify the columns to select; change or set to None to use all columns
    specific_columns = [
        'SubInventory', 'SubInventory Description', 'Item', 'Item Description',
        'Category', 'UOM', 'UOM Name', 'Quantity', 'Unit Cost', 'Extended Value'
    ]

    # Process each combination of year and business unit
    combined_dfs = []
    for year in YEAR_TO_PROCESS:
        for bu in BU_TO_PROCESS:
            input_folder = os.path.join(inv_result_root, year, bu, "INV_VALUE")
            print(f"\nProcessing folder: {input_folder} (Year: {year}, BU: {bu})")
            
            # Check if the folder exists; if not, skip it
            if not os.path.exists(input_folder):
                print(f"Folder {input_folder} does not exist, skipping...")
                continue
            
            # Determine the month from one of the Excel file names in the folder (if any) and differenly by columns
            excel_files = glob.glob(os.path.join(input_folder, "*.xlsx"))
            if excel_files:
                # Extract month from each file in the folder
                month = extract_month_from_filename(os.path.basename(excel_files[0]))
            else:
                month = "Unknown"
            # Debug print showing the input folder, year, BU, and extracted month
            print(f"\nProcessing folder: {input_folder} (Year: {year}, BU: {bu}, Month:{month})")
            
            # Set a unique output filename for all selected BU (and year)
            output_filename = f"combined.parquet"
            
            # Combine Excel files from the current folder and add extra column 'BU' and add extra column 'Month'
            df = combine_excel_files_to_parquet(
                input_folder,
                output_folder,
                output_filename=output_filename,
                custom_columns=specific_columns,
                extra_columns={"BU": bu, "Month": month}
            )
            combined_dfs.append(df)
        


# %%
