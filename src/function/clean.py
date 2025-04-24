import pandas as pd
import json
import sys
from function.config import load_config

# Load configurations from the JSON file
config_path = "/Users/jinjuthatedcharoen/Documents/PPG/P'Aim/DM analysis-Prescription v2/Drug-Prescription/src/version6/config.json"
config = load_config(config_path)

COLUMN_RENAME_CONFIG = config['column_rename_config']
VALUE_REPLACE_CONFIG = config['value_replace_config']

COLUMN_RENAME_DRUG_RECEIVE_CONFIG = config['column_rename_drug_receive_config']

def rename_columns(data, folder):
    for key, rename_dict in COLUMN_RENAME_CONFIG.items():
        if key in folder:
            data = data.rename(columns=rename_dict)
    return data

# def replace_values(data, folder):
#     for key, replace_dict in VALUE_REPLACE_CONFIG.items():
#         if key in folder:
#             for column, replacements in replace_dict.items():
#                 data[column] = data[column].replace(replacements)
#     return data

def replace_values(data, folder, subfolder):
    if subfolder == "HN":
        return data
    
    for key, replace_dict in VALUE_REPLACE_CONFIG.items():
        if key in folder:
            for column, replacements in replace_dict.items():
                if column in data.columns:
                    data[column] = data[column].replace(replacements)
    return data

def change_data_types(data, subfolder):
    # print("Columns before changing data types:", data.columns)  # Debugging
    
    if 'Med_Dose' in data.columns:
        data['Med_Dose'] = data['Med_Dose'].astype(str)
    if 'Right Code' in data.columns:
        data['Right Code'] = data['Right Code'].astype(str)
    if 'Clinic' in data.columns:
        data['Clinic'] = data['Clinic'].astype(str)
    if 'VisitDate' in data.columns:
        data['VisitDate'] = pd.to_datetime(data['VisitDate'], origin='1899-12-30', unit='D')
    if 'AppointmentDatetime' in data.columns:
        data['AppointmentDatetime'] = pd.to_numeric(data['AppointmentDatetime'], errors='coerce')
        data['AppointmentDatetime'] = pd.to_datetime(data['AppointmentDatetime'], origin='1899-12-30', unit='D', errors='coerce')
    
    if 'Finish_Medicine' in data.columns:
        if 'Hospital Site' in data.columns and data['Hospital Site'].isin(['PT3', 'PLS']).any():
            data['Finish_Medicine'] = pd.to_datetime(data['Finish_Medicine'], errors='coerce')
        else:
            data['Finish_Medicine'] = pd.to_numeric(data['Finish_Medicine'], errors='coerce')
            data['Finish_Medicine'] = pd.to_datetime(data['Finish_Medicine'], origin='1899-12-30', unit='D', errors='coerce')
            
    if 'VN' in data.columns:
        data['VN'] = data['VN'].astype(str)
        
    if subfolder == "HN":
        if 'VISITDATE' in data.columns:
            data['VISITDATE'] = pd.to_datetime(data['VISITDATE'], origin='1899-12-30', unit='D')
        if 'FirstDateClinic' in data.columns:
            data['FirstDateClinic'] = pd.to_datetime(data['FirstDateClinic'], origin='1899-12-30', unit='D')
        
    # print("Columns after changing data types:", data.columns)  # Debugging
    return data

def rename_spen_drug_receive_columns(data, folder):
    # print(f"Received folder: '{folder}'")  # Debugging line
    if folder in COLUMN_RENAME_DRUG_RECEIVE_CONFIG:
        rename_dict = COLUMN_RENAME_DRUG_RECEIVE_CONFIG[folder]
        # print(f"Renaming columns for {folder} using {rename_dict}")
        data = data.rename(columns=rename_dict)
        # print(f"After renaming: {data.columns}")
    else:
        print(f"No renaming rules for {folder}")
    return data

def change_spen_drug_receive_data_types(data):
    # print("Columns before changing data types:", data.columns)  # Debugging
    if 'Clinic' in data.columns:
        data['Clinic'] = data['Clinic'].astype(str)
    if 'VisitDate' in data.columns:
        data['VisitDate'] = pd.to_datetime(data['VisitDate'], origin='1899-12-30', unit='D')
    if 'AppointmentDatetime' in data.columns:
        data['AppointmentDatetime'] = pd.to_numeric(data['AppointmentDatetime'], errors='coerce')
        data['AppointmentDatetime'] = pd.to_datetime(data['AppointmentDatetime'], origin='1899-12-30', unit='D', errors='coerce')
    if 'VN' in data.columns:
        data['VN'] = data['VN'].astype(str)
        
    # print("Columns after changing data types:", data.columns)  # Debugging
    return data

def change_hn_data_types(data):
    # print("Columns before changing data types:", data.columns)  # Debugging
    if 'Clinic' in data.columns:
        data['Clinic'] = data['Clinic'].astype(str)
    if 'VISITDATE' in data.columns:
        data['VISITDATE'] = pd.to_datetime(data['VISITDATE'], origin='1899-12-30', unit='D')
    if 'CreatePatient' in data.columns:
        data['CreatePatient'] = pd.to_datetime(data['CreatePatient'], origin='1899-12-30', unit='D')
    if 'FirstDateClinic' in data.columns:
        data['FirstDateClinic'] = pd.to_datetime(data['FirstDateClinic'], origin='1899-12-30', unit='D')
    if 'VN' in data.columns:
        data['VN'] = data['VN'].astype(str)

    # print("Columns after changing data types:", data.columns)  # Debugging
    return data

def remove_duplicate_row(data):
    data = data.drop_duplicates(keep='first')
    return data

def process_data(data, folder, subfolder):
    data = rename_columns(data, folder)
    data = replace_values(data, folder, subfolder)
    data['from_report'] = subfolder
    data = change_data_types(data, subfolder)
    data = remove_duplicate_row(data)
    return data

def process_spen_drug_receive_data(data, folder):
    data = rename_spen_drug_receive_columns(data, folder)
    data = change_spen_drug_receive_data_types(data)
    data = remove_duplicate_row(data)
    return data

def process_hn_data(data):
    data = change_hn_data_types(data)
    data = remove_duplicate_row(data)
    return data

# def clean_colname(df):
#     #force column names to be lower case, no spaces, no dashes
#     df.columns = [x.lower().replace(" ", "_").replace("-","_").replace("\\","_").replace(".","_").replace("$","").replace("%","") for x in df.columns]
#     #processing data
#     replacements = {
#         'timedelta64[ns]': 'varchar',
#         'object': 'varchar',
#         'float64': 'float',
#         'int64': 'int',
#         'datetime64': 'timestamp'
#     }
#     col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(df.columns, df.dtypes.replace(replacements)))
#     return col_str, df.columns

def select_columns(df, columns):
    """Select specific columns from a DataFrame."""
    return df[columns]

def rename_pharma(df, columns_dict):
    """Rename columns in a DataFrame based on a dictionary of new column names."""
    return df.rename(columns=columns_dict)
