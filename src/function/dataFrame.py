import pandas as pd
import numpy as np

def get_Drug_data(df):

    # condition1 = df['Item Type'] == "Drug"
    # condition2 = df["Item Code"].str.startswith("PT")  
    # condition3 = (df['Item Code'] != '99') & (df['Item Code'] != 'P00000000000')  # No drug
    # condition4 = (df['AppointmentDatetime'] != 'no value')
    condition1 = df['receive_drug'] == 1
    condition2 = df['Item Type'] == "Drug" # transaction receive_drug 
    # condition3 = df["Item Code"].str.startswith("PT") # Tablet/Capsule
    condition3 = df["Item Code"].str.startswith("PT") | df["Item Code"].str.startswith("ST")

    
    # Filtering DataFrame based on conditions
    receive_drug_by_visit = df[condition1 & condition2]  # new drug
    receive_other_by_visit = df[~(condition1 & condition2)]  # new no drug
    
    receive_drug_tc_by_visit = df[condition1 & condition2 & condition3]  # new drug
    receive_drug_other_by_visit = df[~(condition1 & condition2 & condition3)]  # new no drug
    
    # Drug = df[condition1 & condition2]  # include No drug
    # NoDrug = df[~(condition1 & condition2 & condition3)] 
    # Drug_test = df[condition1]
    # Drug_check_clinic = df[condition1 & condition2 & condition3]

    return {
        'receive_drug_by_visit': receive_drug_by_visit,
        'receive_other_by_visit': receive_other_by_visit,
        'receive_drug_tc_by_visit': receive_drug_tc_by_visit,
        'receive_drug_other_by_visit': receive_drug_other_by_visit
    }
    
    
def get_Drug_data_from_receive_spen_drug(df):

    # condition1 = df['Received Drug'] == 1
    condition1 = df['Revised Receive Drug'] == 1
    receive_drug = df[condition1]
    receive_no_drug = df[~(condition1)]
    
    return {
        'receive_drug': receive_drug,
        'receive_no_drug': receive_no_drug
    }

def get_Item_Use(df):
    condition1 = df["Item Use"] == 1
    
    # Filtering DataFrame based on the condition
    tc_item_use = df[condition1]      
    tc_item_other = df[~condition1]   
    
    return {
        'tc_item_use': tc_item_use,
        'tc_item_other': tc_item_other
    }

def get_Drug_Appt(df):

    # Condition to select rows where 'AppointmentDatetime' is not blank (not null and not zero)
    condition1 = (df['Has_Appointment'] != 0) & df['Has_Appointment'].notna()

    # Filtering DataFrame based on the condition
    Appt = df[condition1]      # Rows where 'AppointmentDatetime' is not blank
    NoAppt = df[~condition1]   # Rows where 'AppointmentDatetime' is blank
    
    return {
        'Appt': Appt,
        'NoAppt': NoAppt
    }
    
