import pandas as pd

def filter_ST(df):
    # Keep only the rows where 'Item Code' does not start with 'ST'
    filtered_df = df[~df['Item Code'].str.startswith('ST')]
    removed_ST = df[df['Item Code'].str.startswith('ST')]
    return filtered_df, removed_ST

def filter_drug_record(df):
    '''
        PLS [Qty] == 0.1 & [Amt] == 1 >> Drug Record
            [Qty] == 0.01 [Amt] ไม่มี pattern
            
        PT3 [Qty] == 1 and finish within a day [Amt] ไม่มี pattern
        
        ## Store in receive_drug_other dataframe ##
        PTS [Item Code] == 111 recordไม่จ่ายยาเพิ่ม
            [Item Code] == 000 ไม่การจ่ายยา record
    '''
    # Filter out rows where 'Hospital Site' is 'PLS' and 'Qty' is 0.1
    filtered_drug_record = df[~((df['Hospital Site'] == 'PLS') & (df['Qty'] == 0.1))]
    removed_drug_record = df[((df['Hospital Site'] == 'PLS') & (df['Qty'] == 0.1))]
    return filtered_drug_record, removed_drug_record

def filter_right(df):
    # Apply the filter only to non-null values in 'Right Name'
    filtered_right = df[df['Right Name'].isna() | (~df['Right Name'].str.startswith(('ครอบครัว', 'พนักงาน', 'แพทย์', 'กรรมการ ผู้บริหาร', 'คลินิกพนักงาน', 'ตรวจสุขภาพพนักงาน')).fillna(False))]
    removed_right = df[~df['Right Name'].isna() & df['Right Name'].str.startswith(('ครอบครัว', 'พนักงาน', 'แพทย์', 'กรรมการ ผู้บริหาร', 'คลินิกพนักงาน', 'ตรวจสุขภาพพนักงาน'))]
    return filtered_right, removed_right

def filter_payor(df):
    # Define the list of ( employee docotr ) payor values to filter out
    payor_to_filter = [
        "PLSDF-0003-000",
        "PLSEM-0001-C00",
        "PLSEM-0001-000",
        "PLSRE-0001-002",
        "PTNEM-0007-A01",
        "PTNEM-0001-A01",
        "PTNEM-0002-A01",
        "PTNEM-0003-A01",
        "PTS411-EM-0001-AA1",
        "PTS411-EM-0001-AB1",
        "PTS411-EM-0001-DA1",
        "PTS411-DF-0001-B12"
    ]
    
    # Create a unique value by concatenating 'Hospital Site' and 'Payor'
    df['Unique_Value'] = df['Hospital Site'] + df['Payor Code']
    
    # Filter the DataFrame to exclude rows where 'Unique_Value' is in the payor_to_filter list
    filtered_payor = df[~df['Unique_Value'].isin(payor_to_filter)]
    
    # Keep the removed payor values in a separate DataFrame for later review
    removed_payor = df[df['Unique_Value'].isin(payor_to_filter)]
    
    # Drop the temporary 'Unique_Value' column before returning the results
    filtered_payor = filtered_payor.drop(columns=['Unique_Value'])
    removed_payor = removed_payor.drop(columns=['Unique_Value'])
    
    return filtered_payor, removed_payor

def filter_less_than_0_out(df, column):
    filtered_amt_0_out = df[df[column] >= 0]
    removed_amt_0_out = df[df[column] < 0]
    return filtered_amt_0_out, removed_amt_0_out

def filter_other_stat_drug(df):
    filterd_New_Med_Day_is_0 = df[df['New_Med_Day'] != 0]
    removed_New_Med_Day_is_0 = df[df['New_Med_Day'] == 0]
    return filterd_New_Med_Day_is_0, removed_New_Med_Day_is_0

def filter_scope(df):
    # Define the conditions for filtering
    condition_age = (df['AgeYear'] >= 0) & (df['AgeYear'] <= 150)
    condition_appt_days = (df['Appt_Days'] >= 1) & (df['Appt_Days'] <= 365)
    condition_med_day = (df['New_Med_Day'] >= 1) & (df['New_Med_Day'] <= 365)

    # Combine all conditions
    combined_condition = condition_age & condition_appt_days & condition_med_day

    # Create filtered DataFrame
    filtered_scope = df[combined_condition]

    # Create DataFrame of removed rows
    removed_scope = df[~combined_condition]

    return filtered_scope, removed_scope

# def filter_by_years(df, year_column, years):
#     filtered_df = df[df[year_column].dt.year.astype(str).isin(map(str, years))]
#     return filtered_df

def filter_by_years(df, date_col, years):
    """
    Return rows from df where df[date_col] is in the given set of years.
    """
    return df[df[date_col].dt.year.isin(years)]