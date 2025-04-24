#%%
import sys
import time
# Test
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
    
# --------------------------------------------------------------------
# 1) K.Kae PLC 2024 script: process SALE, HISMIC, ORCMII
# --------------------------------------------------------------------

if __name__ == "__main__":

    # Input folder with .xls (tab-delimited) files
    input_folder = config["plc_sale_folder_path"]

    # Define patterns and corresponding output folders
    patterns_and_outputs = [
        ("PLC_SALE_*.xls", config["plc_sale_convert_folder_path"]),
        ("PLC_HISMIC_*.xls", config["plc_hismic_convert_folder_path"]),
        ("PLC_ORCMII_*.xls", config["plc_orcmii_convert_folder_path"])
    ]
    
    # Create each output folder if it doesn't exist
    for pattern, out_folder in patterns_and_outputs:
        os.makedirs(out_folder, exist_ok=True)
        
        # Find all matching files in the input folder
        xls_files = glob.glob(os.path.join(input_folder, pattern))
        
        # Convert each file
        for file_path in xls_files:
            convert_thai_file_to_xlsx(file_path, out_folder)

    print("\nAll files (SALE, HISMIC, ORCMII) have been converted successfully!")


end_time = time.time()
elapsed_time = end_time - start_time
elapsed_minutes = elapsed_time / 60

print(f"Execution time: {elapsed_minutes:.2f} minutes")

# %%
