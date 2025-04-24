import pandas as pd
import os
from function.import_data import *

def load_filter_and_merge_data(file_paths, year_filters):
    """
    Load multiple Excel files, filter each by VisitDate year if a year filter is provided, and merge them.
    If 'None' is provided as a filter, no year filtering is done for that file.
    """
    merged_data = pd.DataFrame()
    
    for file_path, year_filter in zip(file_paths, year_filters):
        if file_path:  # Ensure the file path is not empty
            data = pd.read_excel(file_path)
            
            # If year_filter is an integer, apply the year filter
            if isinstance(year_filter, int):
                data['VisitDate'] = pd.to_datetime(data['VisitDate'])
                data = data[data['VisitDate'].dt.year == year_filter]
            
            # Append data without filtering if year_filter is None
            merged_data = pd.concat([merged_data, data], ignore_index=True)
    
    return merged_data

def combine_df(dfs, axis=0):
    """Combine multiple DataFrames along a specified axis (0 for rows, 1 for columns)."""
    return pd.concat(dfs, axis=axis, ignore_index=True)

# Function to combine all Excel files in the folder
def load_and_combine_excel(folder_path):
    """
    Load all Excel files in the given folder and concatenate them into a single DataFrame.
    """
    # List all Excel files in the folder
    excel_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.xlsx')]
    if not excel_files:
        raise FileNotFoundError(f"No Excel files found in folder: {folder_path}")
    
    # Read all Excel files into DataFrames
    dataframes = [pd.read_excel(file) for file in excel_files]
    
    # Combine all DataFrames
    return pd.concat(dataframes, ignore_index=True)

def process_spen_payor_files(hospital_sites, config):
    """
    Process payor files for multiple hospital sites.
    
    Parameters:
        hospital_sites (list): List of hospital site prefixes (e.g., ['PT1', 'PT2', 'PT3']).
        config (dict): Configuration dictionary with file paths for each site.
        
    Returns:
        dict: Dictionary of combined DataFrames for each site.
    """
    combined_dataframes = {}

    for site in hospital_sites:
        # Get input and output paths from the config
        payor_path = config[f'{site}SpenPayor']
        combined_path = config[f'combined_{site}SpenPayor']
        
        # Load and combine Excel files for the site
        payor_data = load_and_combine_excel(payor_path)
        
        # Convert VN to text for values starting with 'E' or 'TV'
        if 'VN' in payor_data.columns:
            payor_data['VN'] = payor_data['VN'].astype(str)
            
        # Rename 'ClinicCode' to 'Clinic' for site 'PLS'
        if site == "PLS" and 'ClinicCode' in payor_data.columns:
            payor_data.rename(columns={'ClinicCode': 'Clinic'}, inplace=True)
        
        # Ensure 'Clinic' column is present and convert to string for all sites
        if 'Clinic' in payor_data.columns:
            payor_data['Clinic'] = payor_data['Clinic'].astype(str)
        
        # Convert VisitDate to datetime if the column exists
        if 'VisitDate' in payor_data.columns:
            payor_data['VisitDate'] = pd.to_datetime(
                payor_data['VisitDate'], origin='1899-12-30', unit='D'
            )
        # Ensure 'Clinic' column is present and convert to string for all sites
        if 'Clinic' in payor_data.columns:
            payor_data['Clinic'] = payor_data['Clinic'].astype(str)
        
        # Save the combined data to a parquet file
        payor_data.to_parquet(combined_path)
        
        # Load the parquet file to ensure it's properly saved
        combined_dataframes[site] = load_parquet(combined_path)
        
        print(f"Processed and saved payor data for {site}.")
    
    return combined_dataframes
