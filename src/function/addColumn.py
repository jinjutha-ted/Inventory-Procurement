import pandas as pd
import numpy as np
import re
from datetime import datetime

def insert_columns(df, target_col, columns_to_add, position='before', default_values=None):
    """
    Inserts new columns into the DataFrame at a specified position relative to a target column.

    Parameters:
    - df: DataFrame to modify.
    - target_col: Column name to insert new columns relative to.
    - columns_to_add: List of column names to add.
    - position: 'before' or 'after', indicating where to insert relative to the target column.
    - default_values: Value(s) to fill in the new columns. If None, uses blank values.
                      If a single value is provided, all columns use that value.
                      If a list is provided, it should match the length of columns_to_add.
    """
    # Set default values if not provided
    if default_values is None:
        default_values = [""] * len(columns_to_add)
    elif isinstance(default_values, str) or not isinstance(default_values, list):
        default_values = [default_values] * len(columns_to_add)

    # Calculate the insertion index
    target_index = df.columns.get_loc(target_col)
    insert_index = target_index if position == 'before' else target_index + 1

    # Insert each column at the specified position
    for i, col in enumerate(columns_to_add):
        df.insert(insert_index, col, default_values[i])
        insert_index += 1  # Move the index forward after each insertion

def add_receive_drug_column(df):
    df = df.copy()
    # Ensure VisitDate is treated as a string
    df['VisitDate'] = df['VisitDate'].astype(str)
    
     # Ensure all relevant columns are treated as strings and replace missing values with an empty string
    df['VisitDate'] = df['VisitDate'].astype(str).fillna('')
    df['Hospital Site'] = df['Hospital Site'].astype(str).fillna('')
    df['HN'] = df['HN'].astype(str).fillna('')
    df['Clinic'] = df['Clinic'].astype(str).fillna('')
    df['ClinicName'] = df['ClinicName'].astype(str).fillna('')
    df['Doctor'] = df['Doctor'].astype(str).fillna('')
    df['Doctor Name'] = df['Doctor Name'].astype(str).fillna('')
    
    # # Create a unique identifier by concatenating the columns
    # df['unique_id'] = df['Hospital Site'].astype(str) + df['HN'].astype(str) + df['VisitDate'] + df['Clinic'] + df['ClinicName'] + df['Doctor'] + df['Doctor Name']
    
    df['unique_id'] = df['Hospital Site'].astype(str) + df['HN'].astype(str) + df['VisitDate']
    # Group by the unique identifier and check if any Item_Type is "Drug"
    drug_received = df.groupby('unique_id')['Item Type'].apply(lambda x: (x == 'Drug').any()).astype(int)
    
    # Map the results back to the original DataFrame
    df['receive_drug'] = df['unique_id'].map(drug_received)
    
    # Drop the temporary unique_id column
    df.drop(columns=['unique_id'], inplace=True)
    
    return df


# Function to create a new column [Revised Receive Drug] based on group conditions
def add_update_received_drug(df):
    df = df.copy()
    # Create a unique identifier for each group
    df['unique_id'] = (
        df['Hospital Site'].astype(str) + 
        df['HN'].astype(str) + 
        df['VisitDate'].astype(str) + 
        df['VN'].astype(str) + 
        df['Clinic'].astype(str) + 
        df['ClinicName'].astype(str) + 
        df['Doctor'].astype(str) + 
        df['DoctorName'].astype(str)
    )
    
    # Function to determine the value of [Revised Receive Drug] within each group
    def update_received_drug(group):
        # Add a new column [Revised Receive Drug] based on the group condition
        group['Revised Receive Drug'] = 1 if 1 in group['Received Drug'].values else 0
        return group
    
    # Apply the function to each group based on the unique_id
    df = df.groupby('unique_id', group_keys=False).apply(update_received_drug)
    
    # Convert 'VisitDate' back to datetime type if necessary
    df['VisitDate'] = pd.to_datetime(df['VisitDate'], origin='1899-12-30', unit='D', errors='coerce')
    
    # Drop duplicates based on 'unique_id', keeping the first occurrence
    df = df.drop_duplicates(['unique_id', 'Revised Receive Drug'], keep='first')
    
    # Remove the unique_id column
    df.drop(columns=['unique_id'], inplace=True)
    
    return df



def add_site_op(df):
    df = df.copy()

    # Define the mapping for Hospital Site to operation system detail
    site_op_mapping = {
        'PLR': 'I-MED',
        'PT1': 'SSB 64-bit',
        'PT2': 'SSB 64-bit',
        'PT3': 'SSB 64-bit',
        'PTP': 'SSB 32-bit',
        'PLC': 'I-MED X',
        'PLD': 'I-MED',
        'PLK': 'HomC',
        'PLS': 'SSB 32-bit',
        'PTN': 'SSB 32-bit',
        'PTS': 'SSB 64-bit',
        'PTS 2': 'I-MED+'
    }
    
    # Map the site_op details based on Hospital Site
    df['site_op'] = df['Hospital Site'].map(site_op_mapping)
    
    return df

def add_site_type(df):
    df = df.copy()

    # Define the mapping for Hospital Site to operation system detail
    site_op_mapping = {
        'PLR': 'Premium',
        'PT1': 'Premium',
        'PT2': 'Premium',
        'PT3': 'Premium',
        'PTP': 'Premium',
        'PLC': 'Premium SSO',
        'PLD': 'Premium SSO',
        'PLK': 'Premium SSO',
        'PLS': 'Premium SSO',
        'PTN': 'Premium SSO',
        'PTS': 'Premium SSO',
        'PTS 2': 'Premium SSO'
    }
    
    # Map the site_op details based on Hospital Site
    df['site_type'] = df['Hospital Site'].map(site_op_mapping)
    
    return df

def add_payor_sso(df):
    # Add the new column site_right_name
    df['payor_sso'] = df['Hospital Site'] + df['Payor Code']
    
    payor_sso = {
        "PLSFU-0003-000",
        "PLSFU-0026-000",
        "PLSFU-0008-000",
        "PLSFU-0010-000",
        "PLSFU-0013-000",
        "PLSFU-0014-000",
        "PLSFU-0015-000",
        "PLSGV-0002-000",
        "PLSGV-0002-001",
        "PLSGV-0002-003",
        "PLSGV-0006-000",
        "PTNFU-0002-000",
        "PTNFU-0003-000",
        "PTNFU-0006-000",
        "PTNFU-0009-000",
        "PTNGV-0002-000",
        "PTNGV-0001-000",
        "PTNGV-0003-000",
        "PTNGV-0003-001",
        "PTNGV-0003-002",
        "PTNGV-0003-003",
        "PTNGV-0003-306",
        "PTNGV-0003-B06",
        "PTNGV-0003-C06",
        "PTNGV-0003-D06",
        "PTNGV-0003-E06",
        "PTNGV-0006-001",
        "PTNGV-0007-002",
        "PTNGV-0009-004",
        "PTNGV-0010-005",
        "PTNGV-0011-006",
        "PTNGV-0012-001",
        "PTNGV-0013-001",
        "PTNGV-0003-F06",
        "PTNGV-0003-G06",
        "PTNGV-0003-I06",
        "PTNGV-0003-K06",
        "PTNGV-0003-L06",
        "PTNSO-0001-000",
        "PTS411-FU-0001-000",
        "PTS411-FU-0002-000",
        "PTS411-FU-0007-000",
        "PTS411-GV-0001-000",
        "PTS411-GV-0002-000",
        "PTS411-GV-0002-001",
        "PTS411-GV-0002-002",
        "PTS411-GV-0002-003",
        "PTS411-GV-0002-100",
        "PTS411-GV-0002-101",
        "PTS411-GV-0011-308",
        "PTS411-GV-0012-008"
    }
    
    # Assign 1 if 'payor_sso' is in the set, otherwise 0
    df['Payor SSO'] = df['payor_sso'].apply(lambda x: 1 if x in payor_sso else 0)
    
    return df

def add_columns(df, processing_functions):
    if df is None:
        raise ValueError("The input DataFrame is None. Please check the data processing steps prior to this function.")
    
    # Creating a copy of the dataframe to avoid SettingWithCopyWarning
    df = df.copy()

    for func in processing_functions:
        df = func(df)  # Each func is now ready to be called with df only
    return df

def add_has_appointment_column(df):
    df = df.copy()

    # Create 'Has_Appointment' column: 1 if 'AppointmentDatetime' is not missing, 0 otherwise
    df['Has_Appointment'] = df['AppointmentDatetime'].notna().astype(int)

    return df

def parse_mixed_visitdate(series):
    """
    Convert a column that may contain both standard date-strings 
    and Excel date serial numbers.
    """
    # 1) Identify which rows are numeric (Excel serial dates).
    mask_numeric = pd.to_numeric(series, errors='coerce').notnull()

    # 2) Create a copy so we don’t alter the original series directly
    result = series.copy()

    # 3) Convert the numeric ones using the Excel offset
    result.loc[mask_numeric] = (
        pd.to_datetime("1899-12-30") +
        pd.to_timedelta(result.loc[mask_numeric].astype(float), unit="D")
    )

    # 4) Convert the non-numeric ones with standard parsing
    result.loc[~mask_numeric] = pd.to_datetime(result.loc[~mask_numeric])

    # 5) Return the series as datetime
    return pd.to_datetime(result)

def add_concatenation_columns(df):
    df = df.copy()
    
    # Convert 'VisitDate' to string first, just for concatenation
    df['VisitDate'] = df['VisitDate'].astype(str)
    
    # Create your concatenation columns
    df['Patient'] = df['Hospital Site'] + df['HN']
    df['OPD Visit'] = df['Hospital Site'] + df['HN'] + df['VN'] + df['VisitDate']
    
    # Now convert 'VisitDate' back to datetime, handling both numeric and date strings
    df['VisitDate'] = parse_mixed_visitdate(df['VisitDate'])
    
    return df


def add_concatenation_columns_old(df):
    df = df.copy()
    # Convert 'VisitDate' to string if it's a Timestamp
    df['VisitDate'] = df['VisitDate'].astype(str)
    # Concatenation of Hospital Site and HN
    df['Patient'] = df['Hospital Site'] + df['HN']
    df['OPD Visit'] = df['Hospital Site'] + df['HN'] + df['VN'] + df['VisitDate']
    # df['SiteHN-VN-Item Code'] = df['Hospital Site'] + df['HN'] + df['VisitDate'] + df['Item Code']
    
    # Convert 'VisitDate' back to datetime
    df['VisitDate'] = pd.to_datetime(df['VisitDate'])
    # df['VisitDate'] = (
    # pd.to_datetime('1899-12-30') 
    # + pd.to_timedelta(df['VisitDate'], unit='D')
    # )
    return df

def add_clean_doctor_name(df):
    df = df.copy()

    # Define the exceptions
    exceptions = [
        "ผศ.(พิเศษ)",
        "รศ.(พิเศษ)",
        "เกษตรเสริมวิริยะ(เตชะพงศธร)",
        "กลิ่นสุคนธ์(นิ่มน้อย)"
    ]
    
    # Step 1: Trim parentheses and the word inside, except for specific cases
    def clean_doctor_name(name):
        if name is None:  # Check if the name is None
            return name
        # if name in exceptions:
        #     return name
        
        # If the name matches an exception, return it unchanged
        if any(exception in name for exception in exceptions):
            return name
        
        else:
            # Remove text inside parentheses and extra spaces
            cleaned_name = re.sub(r"\(.*?\)", "", name)
            cleaned_name = re.sub(r"\s{2,}", " ", cleaned_name)  # Replace multiple spaces with a single space
            
            return cleaned_name.strip()  # Remove leading/trailing spaces
    
    df.loc[:, 'CleanedDoctorName'] = df['Doctor Name'].apply(clean_doctor_name)
    
    # # Step 2: Replace specific doctor names with correct ones
    df.loc[:, 'CleanedDoctorName'] = df['CleanedDoctorName'].replace({
        "พญ. ัทธ์ธีรา รอดเจริญ": "พญ.นัทธ์ธีรา  รอดเจริญ",
        "น.พ.": "นพ.",
        "พ.ญ.": "พญ.",
        ". ": "",
        ") ": "",
        "ศ.คลินิกเกียรติคุณ ": "",
        "พลเอก ": "พล.อ.",
        "พล ": "พล.",
        "พญสุทธนารัตน์ อภิวันทนา": "พญ.สุทธนารัตน์ อภิวันทนา",
        "นพสาธิต หวังวัชรกุล": "นพ.สาธิต หวังวัชรกุล",
        "(C-Up)พญ.ศุภดา  เกษตรเสริมวิริยะ(เตชะพงศธร)": "พญ.ศุภดา เกษตรเสริมวิริยะ(เตชะพงศธร)",
        "(OHC)พญ.ศุภดา  เกษตรเสริมวิริยะ(เตชะพงศธร)": "พญ.ศุภดา เกษตรเสริมวิริยะ(เตชะพงศธร)",
        "พญ.ศุภดา  เกษตรเสริมวิริยะ(เตชะพงศธร)": "พญ.ศุภดา เกษตรเสริมวิริยะ(เตชะพงศธร)",
        "ผศ.(พิเศษ)พญ.อิศราพร ตรีสิทธิ์": "ผศ.(พิเศษ) พญ.อิศราพร ตรีสิทธิ์",
        "พญ.นารีลักษณ์  กลิ่นสุคนธ์(นิ่มน้อย)": "พญ.นารีลักษณ์ กลิ่นสุคนธ์(นิ่มน้อย)",
        "ร.อ.น.พ.": "ร.อ.นพ."
    }, regex=False)

    return df

# def replace_med_dose_with_new_med_dose(df, med_dose_file):
#     # Load the Excel file
#     med_dose_df = pd.read_excel(med_dose_file, sheet_name='Dose_PLS')
    
#     # Create a mapping dictionary from Med_Dose to New_MedDose
#     med_dose_mapping = dict(zip(med_dose_df['Med_Dose'], med_dose_df['New_MedDose']))
    
#     # Replace the Med_Dose values in the DataFrame
#     df['Med_Dose'] = df['Med_Dose'].map(med_dose_mapping).fillna(df['Med_Dose'])
    
#     return df

def replace_med_dose_blank_with_1(df, column_to_replace):
    df = df.copy()
    # Fill blank values in the column_to_replace with 1
    df[column_to_replace] = df[column_to_replace].fillna(1)
    
    return df

def replace_med_dose_with_new_med_dose(df, med_dose_file, sheet_name, column_to_replace, new_column_name):
    df = df.copy()
    # Load the Excel file
    med_dose_df = pd.read_excel(med_dose_file, sheet_name=sheet_name)
    
    # Create a mapping dictionary from the specified column to the new column
    med_dose_mapping = dict(zip(med_dose_df[column_to_replace], med_dose_df[new_column_name]))

    # Replace the values in the specified column of the DataFrame
    df[column_to_replace] = df[column_to_replace].map(med_dose_mapping).fillna(df[column_to_replace])
    
    # Remove rows where Med_Dose is 'del'
    df = df[df[column_to_replace] != 'del']
    df_del = df[df[column_to_replace] == 'del']
    
    print(f"Drug data where Med_Dose is 'del': {df_del.shape}")
    # Fill blank values in the column_to_replace with 1
    df[column_to_replace] = df[column_to_replace].fillna(1)
    
    return df

def merge_and_filter_data_with_constants(df, filter_file, sheet_name):
    df = df.copy()
    # Load the filter file
    filter_df = pd.read_excel(filter_file, sheet_name=sheet_name)
    # Check the shape of the DataFrame
    print(f"Shape of df before merge constants: {df.shape}")
    print(f"Shape of filter_df: {filter_df.shape}")
    
    # Check if the DataFrame is empty
    if filter_df.empty:
        print("filter_df is empty")
    else:
        print("filter_df is not empty")
        
    # Optionally, print the first few rows of the DataFrame
    print("First few rows of filter_df:")
    # print(filter_df.head())
    
    # Ensure key columns have the same data type
    df['Hospital Site'] = df['Hospital Site'].astype(str)
    df['Item Code'] = df['Item Code'].astype(str)
    df['UOM'] = df['UOM'].astype(str)
    filter_df['Hospital Site'] = filter_df['Hospital Site'].astype(str)
    filter_df['Item Code'] = filter_df['Item Code'].astype(str)
    filter_df['UOM'] = filter_df['UOM'].astype(str)

    # Remove leading/trailing whitespace from key columns
    df['Hospital Site'] = df['Hospital Site'].str.strip()
    df['Item Code'] = df['Item Code'].str.strip()
    df['UOM'] = df['UOM'].str.strip()
    filter_df['Hospital Site'] = filter_df['Hospital Site'].str.strip()
    filter_df['Item Code'] = filter_df['Item Code'].str.strip()
    filter_df['UOM'] = filter_df['UOM'].str.strip()

    # Create concatenated keys
    df['Key'] = df['Hospital Site'] + df['Item Code'] + df['UOM']
    filter_df['Key'] = filter_df['Hospital Site'] + filter_df['Item Code'] + filter_df['UOM']
    
    # Check for and remove duplicate keys in filter_df
    if filter_df['Key'].duplicated().any():
        num_duplicates = filter_df['Key'].duplicated().sum()
        print(f"Number of duplicate keys found: {num_duplicates}")
        filter_df = filter_df.drop_duplicates(subset='Key')
        print("Duplicates removed from filter DataFrame.")
        print(f"Shape of filter_df after removing duplicates: {filter_df.shape}")
    
    # # Print columns before merging for debugging
    # print("Columns in df before merge:", df.columns)
    # print("Columns in filter_df before merge:", filter_df.columns)
    
    # Merge based on the concatenated keys
    merged_df = pd.merge(df, filter_df, on='Key', suffixes=('', '_filter'))
    
    # # Print columns after merging for debugging
    # print("Columns in merged_df after merge:", merged_df.columns)
    
    # # Check if the required columns are present in the merged DataFrame
    # required_columns = ['ItemUse', 'ConstantQtyUOM', 'UOM_filter']
    # missing_columns = [col for col in required_columns if col not in merged_df.columns]
    
    # if missing_columns:
    #     raise KeyError(f"The following required columns are missing from the merged DataFrame: {missing_columns}")

    # Filter data where [ItemUse] == 1
    filtered_df = merged_df[merged_df['ItemUse'] == 1]

    # Add new columns [ConstantQtyUOM] and [UOM] from the filter file
    filtered_df['UOM'] = filtered_df['UOM_filter']
    
    # Drop unnecessary columns
    filtered_df.drop(columns=[col for col in filtered_df.columns if col.endswith('_filter')], inplace=True)
    filtered_df.drop(columns=['Key'], inplace=True)
    
    return filtered_df


def add_calculated_columns(df):
    df = df.copy()
    
    df['New_Med_Dose'] = df['New_Dose/Day'] * df['New_Dose/Time']
    df['New_Med_Qty'] = df['ConstantQtyUOM'] * df['Qty']
    df['New_Med_Day'] = df['New_Med_Qty'] / df['New_Med_Dose']
    
    # Calculate Next_Appt_Days
    # Ensure 'AppointmentDatetime' and 'VisitDate' are in datetime format
    df['AppointmentDatetime'] = pd.to_datetime(df['AppointmentDatetime'], errors='coerce')
    df['VisitDate'] = pd.to_datetime(df['VisitDate'], errors='coerce')
    df['Appt_Days'] = (df['AppointmentDatetime'] - df['VisitDate']).dt.days
    
    # Calculate Rev/New_Med_Qty
    df['Rev/New_Med_Qty'] = df['Amt'] / df['New_Med_Qty'].replace(0, 1)
    
    
    # Assuming df is your DataFrame and it already contains the 'Appt_Days', 'New_Med_Day', and 'Med_Dose' columns
    df['Medication Increase to 100% day (Qty)'] = ((df['Appt_Days'] * 1 - df['New_Med_Day']) * df['New_Med_Dose']).apply(lambda x: x if x > 0 else 0)
    df['Medication Increase to 50% day (Qty)'] = ((df['Appt_Days'] * 0.5 - df['New_Med_Day']) * df['New_Med_Dose']).apply(lambda x: x if x > 0 else 0)
    df['Medication Increase to 20% day (Qty)'] = ((df['Appt_Days'] * 0.2 - df['New_Med_Day']) * df['New_Med_Dose']).apply(lambda x: x if x > 0 else 0)
    df['Medication Increase to 10% day (Qty)'] = ((df['Appt_Days'] * 0.1 - df['New_Med_Day']) * df['New_Med_Dose']).apply(lambda x: x if x > 0 else 0)
    df['Medication Increase to 5% day (Qty)'] = ((df['Appt_Days'] * 0.05 - df['New_Med_Day']) * df['New_Med_Dose']).apply(lambda x: x if x > 0 else 0)

    
    # Calculate additional columns
    # df['ApptDays - New_Med_Day'] = df['Appt_Days'] - df['New_Med_Day']
    # df['(med-appt)/med*100'] = df['ApptDays - New_Med_Day'] / (df['New_Med_Day'] * 100)
    
    df['%_Med_Pre'] = df['New_Med_Day']*100 / df['Appt_Days'].replace(0, 1)
    df['100_minus_%_Med_Pre'] = 100- df['%_Med_Pre']
    
    # Add 'New_Med_Qty_1tab' column
    df['New_Med_Qty_1tab'] = np.where(df['New_Med_Qty'] == 1, 1, 0)
    
    # # Add 'New_Med_Day_less_than_3d' column
    # df['New_Med_Day_less_than_3d'] = np.where(df['New_Med_Day'] < 3, 1, 0)
    
    # # Add 'New_Med_Day_is_3d' column
    # df['New_Med_Day_is_3d'] = np.where(df['New_Med_Day'] == 3, 1, 0)
    
    # Define the tier conditions
    conditions = [
        (df['%_Med_Pre'] >= 81),
        (df['%_Med_Pre'] >= 61),
        (df['%_Med_Pre'] >= 41),
        (df['%_Med_Pre'] >= 21),
        (df['%_Med_Pre'] >= 5),
        (df['%_Med_Pre'] < 5)
    ]
    
    # Define the corresponding values
    choices = ['81-100%', '61-80%', '41-60%', '21-40%', '5-20%', '< 5%']
    
    # Add the 'Tier' column based on the conditions
    df['Tier'] = np.select(conditions, choices, default='0')
    
    # df['Old/New_Clinic'] = 
    # df['Old/New_Patient'] = 
    
    return df

def add_clinic_year(df, first_date_col, create_patient_col):
    df['ClinicYear'] = (df[first_date_col] - df[create_patient_col]).dt.days / 365.25
    df['ClinicYear'] = df['ClinicYear'].round(1)
    
    return df

def add_patient_year(df, create_patient_col):
    # Extract the year from the 'create_patient_col' and calculate the difference from the current year
    df['PatientYear'] = datetime.now().year - df[create_patient_col].dt.year
    
    return df

def calculate_doctor_performance(df):
    
    # Step 1: Add a binary column indicating if the prescription is low (< 20%)
    df['Low_Pre_Doctor_Flag'] = np.where(df['%_Med_Pre'] < 20, 1, 0)

    # Step 2: Create a 'Year' column from the 'VisitDate' column
    df['Year'] = df['VisitDate'].dt.year

    # Step 3: Calculate the total transaction count of low prescription instances for each doctor, for 2023 and 2024 separately
    df['low_pre_doctor_tran'] = df.groupby(['Unique CleanedDoctorName', 'Year'])['Low_Pre_Doctor_Flag'].transform('sum').fillna(0)

    # Step 4: Calculate the total transaction count for each doctor, for 2023 and 2024 separately
    df['total_doctor_transaction'] = df.groupby(['Unique CleanedDoctorName', 'Year'])['Low_Pre_Doctor_Flag'].transform('count')

    # Step 5: Calculate the percentage of low prescription instances for each doctor and round to two decimal places
    df['%Low_Pre_Doctor_value'] = (df['low_pre_doctor_tran'] / df['total_doctor_transaction']).round(2)

    # Step 6: Assign doctors to performance tiers based on %Low_Pre_Doctor_value
    df['Doctor_Tier'] = np.select(
        [
            df['%Low_Pre_Doctor_value'] == 0.0,  # No low prescriptions
            df['%Low_Pre_Doctor_value'] <= 0.05,  # Good performance group
            (df['%Low_Pre_Doctor_value'] > 0.05) & (df['%Low_Pre_Doctor_value'] <= 0.20),  # Moderate performance group
            df['%Low_Pre_Doctor_value'] > 0.20  # Low performance group
        ],
        ['4', '3', '2', '1'],  # Tiers: '0' for no low prescriptions, '3', '2', '1' for performance groups
        default='Unknown'
    )
    
    # Step 7: Get the mode of the Doctor_Tier for each doctor in each year (most frequent tier)
    Doctor_Tier_Mode = df.groupby(['Year', 'Unique CleanedDoctorName'])['Doctor_Tier'].agg(lambda x: x.mode()[0]).reset_index()

    # Merge the tier summary back to the original DataFrame if needed
    df = pd.merge(df, Doctor_Tier_Mode, on=['Year', 'Unique CleanedDoctorName'], how='left', suffixes=('', '_Mode'))

    return df