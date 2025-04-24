import pandas as pd
import os
import sys
import json
from function.clean import process_data


def convert_xls_to_xlsx(file_path, output_path=None):
    """
    Convert an .xls file with multiple sheets into a single .xlsx workbook.
    
    Parameters:
        file_path (str): Path to the source .xls file.
        output_path (str, optional): Directory to save the output file.
                                     If None, the output file is saved in the same directory.
    
    Returns:
        str: Path to the created .xlsx file, or None if an error occurs.
    """
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    
    # Create the output directory if provided and it doesn't exist
    if output_path and not os.path.isdir(output_path):
        os.makedirs(output_path, exist_ok=True)
    
    # Define the output file name and path
    output_file = os.path.join(output_path, f"{base_name}_converted.xlsx") if output_path else f"{base_name}_converted.xlsx"
    
    try:
        # Open the .xls file using xlrd engine
        excel_file = pd.ExcelFile(file_path, engine='xlrd')
        
        # Create an ExcelWriter object to write to a new .xlsx file
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            for sheet in excel_file.sheet_names:
                # Read each sheet from the .xls file
                df = pd.read_excel(excel_file, sheet_name=sheet, engine='xlrd')
                # Write the DataFrame to the .xlsx file under the same sheet name
                df.to_excel(writer, sheet_name=sheet, index=False)
                
        print(f"Saved all sheets to {output_file}")
        return output_file
    except Exception as e:
        print(f"Error converting {file_path}: {e}")
        return None

# Example usage:
# xlsx_file = convert_xls_to_xlsx("source_data.xls", output_path="converted_files")


# Function to load data from an Excel file
def load_excel(file_path):
    """Load data from an Excel file."""
    try:
        data = pd.read_excel(file_path)
        return data
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return pd.DataFrame()
    
def load_excel_sheetname(file_path, sheet_name=None):
    """Load data from an Excel file, optionally from a specific sheet."""
    try:
        data = pd.read_excel(file_path, sheet_name=sheet_name)
        return data
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return pd.DataFrame()
  
# Function to load data from a CSV file
def load_csv(file_path):
    """Load data from a CSV file."""
    try:
        data = pd.read_csv(file_path)
        return data
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return pd.DataFrame()
    
# Function to load data from a parquet file
def load_parquet(file_path):
    """Load data from a parquet file."""
    try:
        data = pd.read_parquet(file_path)
        return data
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return pd.DataFrame()
    
def load_folders(base_path, subfolder, folders, years, all_data):
    for folder in folders:
        if folder:  # Check if the folder is not an empty string
            for year in years:
                folder_path = os.path.join(base_path, subfolder, folder, year)
                if os.path.exists(folder_path):
                    files = os.listdir(folder_path)
                    for file in files:
                        if file.endswith('.csv') or file.endswith('.xlsx'):  # Handle both CSV and Excel files
                            file_path = os.path.join(folder_path, file)
                            print(f"Processing file: {file}")  # Debugging
                            if file.endswith('.csv'):
                                data = pd.read_csv(file_path)
                            elif file.endswith('.xlsx'):
                                data = pd.read_excel(file_path)
                            
                            # Process the data
                            processed_data = process_data(data, folder, subfolder)
                            
                            # Concatenate processed data to all_data
                            all_data = pd.concat([all_data, processed_data], ignore_index=True)
                            
                else:
                    print(f"Path does not exist: {folder_path}")  # Debugging
    return all_data

# def load_data(base_path, premium_folders, premium_sso_folders, years):
#     all_data = pd.DataFrame()
#     if premium_folders:
#         print(f"Loading PREMIUM folders: {premium_folders}")  # Debugging
#         all_data = load_folders(base_path, 'PREMIUM', premium_folders, years, all_data)
#     if premium_sso_folders:
#         print(f"Loading PREMIUM SSO folders: {premium_sso_folders}")  # Debugging
#         all_data = load_folders(base_path, 'PREMIUM SSO', premium_sso_folders, years, all_data)
#     print("Data combined into DataFrame successfully")
#     return all_data

def load_data(base_path, spen_drug_reiceive_folders, spen_drug_folders, years):
    all_data = pd.DataFrame()
    if spen_drug_reiceive_folders:
        print(f"Loading SpenDrugReceive folders: {spen_drug_reiceive_folders}")  # Debugging
        all_data = load_folders(base_path, 'SpenDrugReceive', spen_drug_reiceive_folders, years, all_data)
    if spen_drug_folders:
        print(f"Loading SpenDrug folders: {spen_drug_folders}")  # Debugging
        all_data = load_folders(base_path, 'SpenDrug', spen_drug_folders, years, all_data)
        all_data = all_data.drop(columns=['from_report'], errors='ignore')  # Drop 'Site Type' column
    print("Data combined into DataFrame successfully")
    return all_data