import pandas as pd
import sys
import os

# Adding the parent directory to sys.path for module imports
sys.path.append('../')

from function.import_data import *
from function.clean import *
from function.addColumn import add_concatenation_columns, add_site_type

def combine_parquet_files(directory):
    # List to hold df
    dataframes = []
    
    # Iterate over all files in the directory
    for file in os.listdir(directory):
        if file.endswith('.parquet'):
            file_path = os.path.join(directory, file)
            df = load_parquet(file_path)
            dataframes.append(df)  # Append the loaded DataFrame to the list
    
    # Concatenate all df
    combined_df = pd.concat(dataframes, ignore_index=True)
    print(f"SpenDrug data combined and saved")
    
    return combined_df

def combine_xlsx_to_parquet(directory):
    """
    Gathers all .xlsx files from '2024' subfolders within site folders ending with 'Spen',
    converts them to a single combined DataFrame, and saves as .parquet if desired.
    """
    site_folders = [
        folder for folder in os.listdir(directory)
        if folder.endswith('Spen') and os.path.isdir(os.path.join(directory, folder))
    ]
    
    dataframes = []

    for site in site_folders:
        subfolder_path = os.path.join(directory, site, "2024")

        if not os.path.exists(subfolder_path):
            print(f"Skipping {site} since the path does not exist: {subfolder_path}")
            continue

        for file_name in os.listdir(subfolder_path):
            if file_name.endswith('.xlsx'):
                file_path = os.path.join(subfolder_path, file_name)
                print(f"Reading: {file_path}")

                # Read the Excel file into a DataFrame
                df = pd.read_excel(file_path)
                dataframes.append(df)

    if not dataframes:
        print("No Excel files found in the '2024' subfolders.")
        return pd.DataFrame()  # return empty DataFrame if nothing found

    combined_df = pd.concat(dataframes, ignore_index=True)
    print("XLSX data combined successfully.")
    return combined_df

def combine_spen_drug_receive_data(raw_data_folder_path, spen_drug_receive_folders, spen_drug_folders, years, output_path):
    combined_data = pd.DataFrame()
    
    for folder in spen_drug_receive_folders:
        for hospital_folder in spen_drug_folders:
            for year in years:
                folder_path = os.path.join(raw_data_folder_path, folder, hospital_folder, year)
                
                if not os.path.exists(folder_path):
                    print(f"Path does not exist: {folder_path}")
                    continue
                
                for file_name in os.listdir(folder_path):
                    if file_name.endswith('.xlsx') or file_name.endswith('.xls'):
                        file_path = os.path.join(folder_path, file_name)
                        print(f"Loading data from: {file_name}")

                        try:
                            data = pd.read_excel(file_path)
                            data = process_spen_drug_receive_data(data, hospital_folder)
                            
                            combined_data = pd.concat([combined_data, data], ignore_index=True)
                        except Exception as e:
                            print(f"Error loading {file_name}: {e}")
    
    if not combined_data.empty:
        # Add patient and OPD Visit Count
        combined_data = add_concatenation_columns(combined_data)
        combined_data = add_site_type(combined_data)
        combined_data.to_excel(output_path)
        print(f"SpenDrugReceive data combined and saved")
    else:
        print("No SpenDrugReceive data was combined. Please check the file paths.")
        
def combine_spen_hn_data(raw_data_folder_path, spen_hn_folders, output_path):
    combined_data = pd.DataFrame()
    
    for hospital_folder in spen_hn_folders:
        folder_path = os.path.join(raw_data_folder_path, hospital_folder)
        
        if not os.path.exists(folder_path):
            print(f"Path does not exist: {folder_path}")
            continue
        
        for file_name in os.listdir(folder_path):
            if file_name.endswith('.xlsx') or file_name.endswith('.xls'):
                file_path = os.path.join(folder_path, file_name)
                print(f"Loading data from: {file_name}")
                
                try:
                    data = pd.read_excel(file_path)
                    data = process_hn_data(data)
                    combined_data = pd.concat([combined_data, data], ignore_index=True)
                except Exception as e:
                    print(f"Error loading {file_name}: {e}")
    
    if not combined_data.empty:
        # Correcting column names for summary count and add patient and OPD Visit Count
        combined_data.rename(columns={
            'Site': 'Hospital Site', 
            'VISITDATE': 'VisitDate'
        }, inplace=True)
        combined_data = add_concatenation_columns(combined_data)
        combined_data.to_parquet(output_path)
        print(f"HN data combined and saved to")
    else:
        print("No HN data was combined. Please check the file paths.")