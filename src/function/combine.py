import os
import glob
import pandas as pd
import re

def extract_month_from_filename(filename: str) -> str:
        """
        Extracts the month from the filename.
        Expects a pattern like 'G5_Inventory_Value_Report___by_APR24' where APR corresponds to the month.
        Returns the month as a two-digit string (e.g., '04') if found; otherwise, returns 'Unknown'.
        """
        month_map = {
            "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04", "MAY": "05", "JUN": "06",
            "JUL": "07", "AUG": "08", "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12"
        }
        m = re.search(r"___by_([A-Z]{3})", filename)
        if m:
            mon_abbr = m.group(1)
            return month_map.get(mon_abbr, "Unknown")
        return "Unknown"

def combine_excel_files_to_parquet(
    input_folder: str,
    output_folder: str,
    output_filename: str = "combined.parquet",
    file_extension: str = "xlsx",
    custom_columns: list = None,
    extra_columns: dict = None
) -> pd.DataFrame:
    """
    Combine multiple Excel files from the input_folder into one DataFrame,
    ensuring column alignment and consistent data types, and export the result
    to a Parquet file in the output_folder.
    
    Steps:
        0. Optionally select custom needed columns if provided.
        1. Recheck if column names are aligned, fixing mismatches.
        2. Recheck data types, converting if needed.
        3. Combine all DataFrames.
        4. Export the combined DataFrame to a Parquet file.
    
    Parameters:
        input_folder (str): Path to the folder containing Excel files.
        output_folder (str): Path to the folder where the combined file will be saved.
        output_filename (str): Name of the output Parquet file. Default is 'combined.parquet'.
        file_extension (str): Extension of the files to search for (default is 'xlsx').
        custom_columns (list): Optional list of columns to select from each file. 
                               If None, all columns in the files will be used.
    
    Returns:
        pd.DataFrame: The combined DataFrame.
    """
    
    # Create the output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # Retrieve the list of Excel files from the input folder
    pattern = os.path.join(input_folder, f"*.{file_extension}")
    excel_files = glob.glob(pattern)
    print(f"Found {len(excel_files)} {file_extension} files in {input_folder}.")
    
    dfs = []              # List to store processed DataFrames
    expected_columns = None  # To store the expected column order
    expected_dtypes = None   # To store the expected data types
    
    for file in excel_files:
        print(f"Processing file: {file}")
        # Read the current Excel file
        df = pd.read_excel(file)
            
        # Step 0: Optionally select custom needed columns if provided
        if custom_columns is not None:
            missing = set(custom_columns) - set(df.columns)
            if missing:
                print(f"Warning: The file {file} is missing columns: {missing}. They will be filled with NaN.")
                for col in missing:
                    df[col] = None
            df = df.loc[:, custom_columns]  # Select and reorder columns
            
        # Step 0.5: Optionally add extra columns with constant values
        if extra_columns is not None:
            for col, value in extra_columns.items():
                if col != "Month":
                    df[col] = value
        
        # NEW: Extract month from the current file's name and add it as a column.
        current_month = extract_month_from_filename(os.path.basename(file))
        df["Month"] = current_month
        
        # For the first file, store expected column names and data types
        if expected_columns is None:
            expected_columns = df.columns
            expected_dtypes = df.dtypes.to_dict()
        else:
            # Step 1: Recheck if column names are aligned, and fix if necessary
            if set(df.columns) != set(expected_columns):
                print(f"Column mismatch found in {file}. Aligning columns...")
                missing_cols = set(expected_columns) - set(df.columns)
                for col in missing_cols:
                    df[col] = None
                extra_cols = set(df.columns) - set(expected_columns)
                if extra_cols:
                    print(f"Dropping extra columns in {file}: {extra_cols}")
                    df = df.drop(columns=list(extra_cols))
                # Reorder columns to match the expected order
                df = df[expected_columns]
        
        # Step 2: Recheck and fix data types for consistency
        for col, expected_type in expected_dtypes.items():
            if df[col].dtype != expected_type:
                try:
                    df[col] = df[col].astype(expected_type)
                except Exception as e:
                    print(f"Warning: Could not convert column '{col}' in {file} to {expected_type}. Error: {e}")
        
        dfs.append(df)
    
    # Step 3: Combine all DataFrames into one
    new_combined_df = pd.concat(dfs, ignore_index=True)
    print(f"New combined DataFrame shape: {new_combined_df.shape}")
    
    # Determine output file path
    output_file = os.path.join(output_folder, output_filename)
    
    # Step 4: Append to existing data if the output Parquet file exists
    if os.path.exists(output_file):
        print("Output file exists. Appending new data to existing data.")
        existing_df = pd.read_parquet(output_file)
        combined_df = pd.concat([existing_df, new_combined_df], ignore_index=True)
    else:
        combined_df = new_combined_df
    
    combined_df.to_parquet(output_file, index=False)
    print(f"Combined DataFrame exported to {output_file}")
    
    return combined_df