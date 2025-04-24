#%%
import sys
import time

# Adding the parent directory to sys.path for module imports
sys.path.append('../')

import pandas as pd
import chardet
import glob
import os
import json
from function.import_data import *
from function.parseThaiDate import *
from function.exportExcel import *

start_time = time.time()

# Load configuration
config_path = "/Users/jinjuthatedcharoen/Documents/PPG/P'Aim/Inventory/Python/src/config.json"
with open(config_path, 'r') as file:
    config = json.load(file)
    
if __name__ == "__main__":
    start_time = time.time()

    data_root   = config["INV_SOURCE_ROOT"]
    result_root = config["INV_CLEAN_ROOT"]

    YEAR_TO_PROCESS = ["ข้อมูลคลังสินค้า 2024","ข้อมูลคลังสินค้า 2025"]   
    BU_TO_PROCESS = ["PLC"] 
    
    # Define base patterns without the file extension.
    base_patterns = [
        ("PYT_รายงานข้อมูลการรับสินค้า*", "INV_REPORT", convert_pipe_delimited_file_to_xlsx, 5),
        ("G5_Inventory_Current_On_Hand*", "INV_ONHAND", convert_pipe_delimited_file_to_xlsx, 0),
        ("G5_Inventory_Value_Report*", "INV_VALUE", convert_pipe_delimited_file_to_xlsx, 4),
        ("PYT_DOS___Sale_Transaction__Ne*", "DOS_SALE", convert_pipe_delimited_file_to_xlsx, 0),
        ("PYT_DOS___Extract_Item_Informa*", "DOS_ITEM", convert_pipe_delimited_file_to_xlsx, 0),
        ("ORCMII JAN-DEC*", "ORCMII", convert_orcmii_file_to_xlsx, 0),
        ("POSMIS JAN-DEC*", "POSMIS", convert_orcmii_file_to_xlsx, 0),
        ("SSBMIC JAN-DEC*", "SSBMIC", convert_orcmii_file_to_xlsx, 0),
        ("HISMIC JAN-DEC*", "HISMIC", convert_orcmii_file_to_xlsx, 0),
        
        ("ORCMII*", "ORCMII", convert_orcmii_file_to_xlsx, 0),
        ("POSMIS*", "POSMIS", convert_orcmii_file_to_xlsx, 0),
        ("SSBMIC*", "SSBMIC", convert_orcmii_file_to_xlsx, 0),
        ("HISMIC*", "HISMIC", convert_orcmii_file_to_xlsx, 0)
    ]

    # Specify the file extensions that want to support.
    extensions = [".xls", ".XLS", ".xlsx", ".XLSX"]

    # Dynamically build the complete patterns with extensions.
    patterns_and_outputs = []
    for base_pattern, category, func, skip in base_patterns:
        for ext in extensions:
            patterns_and_outputs.append((base_pattern + ext, category, func, skip))

    for year_folder in sorted(os.listdir(data_root)):
        if YEAR_TO_PROCESS and year_folder not in YEAR_TO_PROCESS:
            continue
    
        year_path = os.path.join(data_root, year_folder)
        if not os.path.isdir(year_path):
            continue

        year = year_folder.split()[-1]

        for bu in sorted(os.listdir(year_path)):
            # Skip all except the ones you specified
            if BU_TO_PROCESS and bu not in BU_TO_PROCESS:
                continue
    
            bu_path = os.path.join(year_path, bu)
            if not os.path.isdir(bu_path):
                continue

            for pattern, category, func, skip in patterns_and_outputs:
                output_folder = os.path.join(result_root, year, bu, category)
                os.makedirs(output_folder, exist_ok=True)

                for file_path in glob.glob(os.path.join(bu_path, pattern)):
                    func(file_path, output_folder, skip)

    print("\nAll files have been converted successfully!")
    print(f"Execution time: {(time.time() - start_time)/60:.2f} minutes")

# %%
