import pandas as pd
import numpy as np
from scipy.stats import pearsonr
import matplotlib.pyplot as plt
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows

def export_to_excel(df, file_path, index=False):
    with pd.ExcelWriter(file_path, engine='openpyxl', mode='w') as writer:
        df.to_excel(writer, index=index)

# def calculate_subgroup_correlations(df, subgroup, col1, col2):
#     unique_values = df[subgroup].unique()
#     correlation_list = []

#     for value in unique_values:
#         subgroup_df = df[df[subgroup] == value]
#         corr_value = subgroup_df[[col1, col2]].corr(method='pearson').iloc[0, 1]  # Get the Pearson correlation between the two columns
#         subgroup_count = len(subgroup_df)
#         correlation_list.append((value, subgroup_count, corr_value))

#     # Convert to DataFrame for better display
#     correlation_df = pd.DataFrame(correlation_list, columns=[subgroup, 'Count', 'Correlation'])
#     return correlation_df

def calculate_subgroup_correlations(df, subgroup, col1, col2):
    unique_values = df[subgroup].unique()
    correlation_list = []

    for value in unique_values:
        subgroup_df = df[df[subgroup] == value]
        if len(subgroup_df) > 1:  # Ensure there are enough data points to calculate correlation
            corr_value, p_value = pearsonr(subgroup_df[col1], subgroup_df[col2])
        else:
            corr_value, p_value = np.nan, np.nan  # Not enough data to calculate correlation
        subgroup_count = len(subgroup_df)
        correlation_list.append((value, subgroup_count, corr_value, p_value))

    # Convert to DataFrame for better display
    correlation_df = pd.DataFrame(correlation_list, columns=[subgroup, 'Count', 'Correlation', 'p-value'])
    return correlation_df

def plot_boxplots(df, cols, subgroup=None):
    if subgroup:
        unique_values = df[subgroup].unique()
        for value in unique_values:
            subgroup_df = df[df[subgroup] == value]
            plt.figure(figsize=(12, 6))
            subgroup_df[cols].boxplot()
            plt.title(f'Boxplot for {subgroup}: {value}')
            plt.show()
    else:
        plt.figure(figsize=(12, 6))
        df[cols].boxplot()
        plt.title('Boxplot for all data')
        plt.show()

def calculate_nested_subgroup_correlations(df, group1, group2, col1, col2):
    unique_group1_values = df[group1].unique()
    correlation_list = []

    for group1_value in unique_group1_values:
        group1_df = df[df[group1] == group1_value]
        unique_group2_values = group1_df[group2].unique()

        for group2_value in unique_group2_values:
            subgroup_df = group1_df[group1_df[group2] == group2_value]
            if subgroup_df[[col1, col2]].dropna().shape[0] > 1:  # Ensure at least two non-NA rows
                corr_value = subgroup_df[[col1, col2]].corr().iloc[0, 1]
            else:
                corr_value = np.nan  # Not enough data to calculate correlation
            subgroup_count = len(subgroup_df)
            correlation_list.append((group1_value, group2_value, subgroup_count, corr_value))

    correlation_df = pd.DataFrame(correlation_list, columns=[group1, group2, 'Count', 'Correlation'])
    return correlation_df

def calculate_doctor_hn_counts(df, hospital_site_column='Hospital Site', doctor_column='Doctor Name', patient_column='HN'):
    # Create a unique identifier by concatenating 'Hospital Site' and 'Doctor Name'
    df['Site_Doctor'] = df[hospital_site_column] + '_' + df[doctor_column]
    # Group by the unique identifier and count distinct 'HN's
    doctor_hn_counts = df.groupby('Site_Doctor')[patient_column].nunique().reset_index(name='HN')
    # Separate the 'Site_Doctor' back into 'Hospital Site' and 'Doctor Name'
    doctor_hn_counts[hospital_site_column], doctor_hn_counts[doctor_column] = zip(*doctor_hn_counts['Site_Doctor'].apply(lambda x: x.split('_')))
    return doctor_hn_counts.drop(columns='Site_Doctor')

def save_all_correlations_to_excel(df, subgroups, nested_groups, col1, col2, output_file_path):
    with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
        overall_count = len(df)
        unique_patient_count = df['HN'].nunique()

        # Save the regular subgroup correlations with added counts
        for subgroup in subgroups:
            subgroup_correlations = calculate_subgroup_correlations(df, subgroup, col1, col2)
            # Calculate the number of unique patients for each subgroup
            subgroup_patient_counts = df.groupby(subgroup)['HN'].nunique().reset_index(name='Patients')
            # Merge the counts with the correlations
            subgroup_correlations = subgroup_correlations.merge(subgroup_patient_counts, on=subgroup, how='left')
            # Add Transactions and Overall count columns
            subgroup_correlations['Transactions'] = subgroup_correlations['Count']
            subgroup_correlations['Overall_Count'] = overall_count
            # Add new columns
            subgroup_correlations['Correlation (r)'] = subgroup_correlations['Correlation']
            subgroup_correlations['Square Correlation (r^2)'] = subgroup_correlations['Correlation'] ** 2
            subgroup_correlations['Correlation*100 (r*100)'] = subgroup_correlations['Correlation'] * 100
            # Reorder columns to match the request
            subgroup_correlations = subgroup_correlations[[subgroup, 'Patients', 'Transactions', 'Overall_Count', 'Correlation (r)', 'Square Correlation (r^2)', 'Correlation*100 (r*100)']]
            subgroup_correlations.to_excel(writer, sheet_name=subgroup[:31], index=False)  # Sheet name is limited to 31 characters

    #     # Save the category correlation charts data
    #     category_correlation_data = get_category_correlation_data(df, subgroups, col1, col2)
    #     category_correlation_data.to_excel(writer, sheet_name='Category_Correlations', index=False)

    #     # Calculate and save the nested subgroup correlations
    #     nested_correlations = calculate_nested_subgroup_correlations(df, nested_groups[0], nested_groups[1], col1, col2)
    #     # Add Patients column for the 'Doctor Name' subgroup
    #     if 'Doctor Name' in nested_groups:
    #         doctor_patient_counts = calculate_doctor_hn_counts(df)
    #         nested_correlations = nested_correlations.merge(doctor_patient_counts, on=nested_groups, how='left')
    #     nested_correlations.to_excel(writer, sheet_name='Nested_Correlations', index=False)

    # print(f"All correlations have been saved to {output_file_path}")
    
        # Calculate and save the nested subgroup correlations
        nested_correlations = calculate_nested_subgroup_correlations(df, nested_groups[0], nested_groups[1], col1, col2)
        # Add Patients column for the nested subgroups
        for group in nested_groups:
            # nested_patient_counts = df.groupby(nested_groups)['HN'].nunique().reset_index(name='Patients')
            # nested_correlations = nested_correlations.merge(nested_patient_counts, on=nested_groups, how='left')
            if group == 'Doctor Name':
                doctor_patient_counts = calculate_doctor_hn_counts(df)
                nested_correlations = nested_correlations.merge(doctor_patient_counts, on=group, how='left')
                
        # Add Transactions and Overall count columns to nested correlations
        nested_correlations['Transactions'] = nested_correlations['Count']
        nested_correlations['Overall_Count'] = overall_count
        nested_correlations['Correlation (r)'] = nested_correlations['Correlation']
        nested_correlations['Square Correlation (r^2)'] = nested_correlations['Correlation'] ** 2
        nested_correlations['Correlation*100 (r*100)'] = nested_correlations['Correlation'] * 100
        
        # # Reorder columns to match the request
        # nested_correlations = nested_correlations[nested_groups + ['Patients', 'Transactions', 'Overall_Count', 'Correlation (r)', 'Square Correlation (r^2)', 'Correlation*100 (r*100)']]
        
        nested_correlations.to_excel(writer, sheet_name='Nested_Correlations', index=False)

    print(f"All correlations have been saved to {output_file_path}")
    
def save_all_custom_correlations_to_excel(df, subgroups, nested_groups, col1, col2, output_file_path):
    with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
        overall_count = len(df)
        unique_patient_count = df['HN'].nunique()

        # Save the regular subgroup correlations with added counts
        for subgroup in subgroups:
            subgroup_correlations = calculate_subgroup_correlations(df, subgroup, col1, col2)
            # Calculate the number of unique patients for each subgroup
            subgroup_patient_counts = df.groupby(subgroup)['HN'].nunique().reset_index(name='Patients')
            # Merge the counts with the correlations
            subgroup_correlations = subgroup_correlations.merge(subgroup_patient_counts, on=subgroup, how='left')
            # Add Transactions and Overall count columns
            subgroup_correlations['Transactions'] = subgroup_correlations['Count']
            subgroup_correlations['Overall_Count'] = overall_count
            # Add new columns
            subgroup_correlations['Correlation (r)'] = subgroup_correlations['Correlation']
            subgroup_correlations['Square Correlation (r^2)'] = subgroup_correlations['Correlation'] ** 2
            subgroup_correlations['Correlation*100 (r*100)'] = subgroup_correlations['Correlation'] * 100
            # Reorder columns to match the request
            subgroup_correlations = subgroup_correlations[[subgroup, 'Patients', 'Transactions', 'Overall_Count', 
                                                           'Correlation (r)', 'Square Correlation (r^2)', 'Correlation*100 (r*100)']]
            subgroup_correlations.to_excel(writer, sheet_name=subgroup[:31], index=False)  # Sheet name is limited to 31 characters

        # Calculate and save the nested subgroup correlations
        nested_correlations = calculate_nested_subgroup_correlations(df, nested_groups[0], nested_groups[1], col1, col2)

        # Add Patients column for the nested subgroups
        for group in nested_groups:
            if group == 'Clinic':
                # Calculate patients count for each Clinic
                clinic_patient_counts = df.groupby(['Hospital Site', 'Clinic'])['HN'].nunique().reset_index(name='Patients')
                nested_correlations = nested_correlations.merge(clinic_patient_counts, on=['Hospital Site', 'Clinic'], how='left')
            elif group == 'Doctor Name':
                # Calculate patients count for each Doctor Name
                doctor_patient_counts = df.groupby(['Hospital Site', 'Clinic', 'Doctor Name'])['HN'].nunique().reset_index(name='Patients')
                nested_correlations = nested_correlations.merge(doctor_patient_counts, on=['Hospital Site', 'Clinic', 'Doctor Name'], how='left')

        # Add Transactions and Overall count columns to nested correlations
        nested_correlations['Transactions'] = nested_correlations['Count']
        nested_correlations['Overall_Count'] = overall_count
        nested_correlations['Correlation (r)'] = nested_correlations['Correlation']
        nested_correlations['Square Correlation (r^2)'] = nested_correlations['Correlation'] ** 2
        nested_correlations['Correlation*100 (r*100)'] = nested_correlations['Correlation'] * 100
        
        # Reorder columns to include all nested groups dynamically
        nested_correlations = nested_correlations[nested_groups + ['Patients', 'Transactions', 'Overall_Count', 
                                                                   'Correlation (r)', 'Square Correlation (r^2)', 'Correlation*100 (r*100)']]
        
        nested_correlations.to_excel(writer, sheet_name='Nested_Correlations', index=False)

    print(f"All correlations have been saved to {output_file_path}")
    
def get_category_correlation_data(df, subgroups, col1, col2):
    # This function will replicate the logic from plot_category_correlation_charts
    # to gather the data in a DataFrame format for Excel export
    category_data_list = []
    df_filtered = df[df['Category'] != 'ไม่ระบุ']
    unique_categories = df_filtered['Category'].unique()
    overall_count = len(df_filtered)

    for category in unique_categories:
        category_df = df_filtered[df_filtered['Category'] == category]
        category_patient_count = category_df['HN'].nunique()
        category_transaction_count = len(category_df)
        corr_value = category_df[[col1, col2]].corr().iloc[0, 1]

        category_data_list.append({
            'Category': category,
            'Patients': category_patient_count,
            'Transactions': category_transaction_count,
            'Overall_Count': overall_count,
            'Correlation (r)': corr_value,
            'Square Correlation (r^2)': corr_value ** 2,
            'Correlation*100 (r*100)': corr_value * 100
        })

    return pd.DataFrame(category_data_list)













def save_all_correlations_to_excel_01(df, subgroups, nested_groups, col1, col2, output_file_path):
    with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
        overall_count = len(df)
        unique_patient_count = df['HN'].nunique()

        # Save the regular subgroup correlations with added counts
        for subgroup in subgroups:
            subgroup_correlations = calculate_subgroup_correlations(df, subgroup, col1, col2)
            # Calculate the number of unique patients for each subgroup
            subgroup_patient_counts = df.groupby(subgroup)['HN'].nunique().reset_index(name='Patients')
            # Merge the counts with the correlations
            subgroup_correlations = subgroup_correlations.merge(subgroup_patient_counts, on=subgroup, how='left')
            # Add Transactions and Overall count columns
            subgroup_correlations['Transactions'] = subgroup_correlations['Count']
            subgroup_correlations['Overall_Count'] = overall_count

            # Add new columns
            subgroup_correlations['Correlation (r)'] = subgroup_correlations['Correlation']
            subgroup_correlations['Square Correlation (r^2)'] = subgroup_correlations['Correlation'] ** 2
            subgroup_correlations['Correlation*100 (r*100)'] = subgroup_correlations['Correlation'] * 100

            # Reorder columns to match the request
            columns_to_keep = [subgroup, 'Patients', 'Transactions', 'Overall_Count', 'Correlation (r)', 'Square Correlation (r^2)', 'Correlation*100 (r*100)']
            subgroup_correlations = subgroup_correlations[columns_to_keep]
            # When saving to Excel, use the first 31 characters of the subgroup for the sheet name
            sheet_name = subgroup[:31] if len(subgroup) > 31 else subgroup
            subgroup_correlations.to_excel(writer, sheet_name=sheet_name, index=False)

        # Save the category correlation charts data
        category_correlation_data = get_category_correlation_data(df, subgroups, col1, col2)
        category_correlation_data.to_excel(writer, sheet_name='Category_Correlations', index=False)

        # Calculate and save the nested subgroup correlations
        nested_correlations = calculate_nested_subgroup_correlations01(df, nested_groups[0], nested_groups[1], nested_groups[2], col1, col2)
        # Add Patients column for the nested subgroups
        for group in nested_groups:
            if group == 'Doctor Name':  # This assumes that doctor counts are only relevant when 'Doctor Name' is part of the nesting
                doctor_patient_counts = calculate_doctor_hn_counts(df)
                nested_correlations = nested_correlations.merge(doctor_patient_counts, on=group, how='left')
        nested_correlations.to_excel(writer, sheet_name='Nested_Correlations', index=False)

    print(f"All correlations have been saved to {output_file_path}")

def calculate_nested_subgroup_correlations01(df, group1, group2, group3, col1, col2):
    # Check if all the required columns exist in the DataFrame
    for column in [group1, group2, group3, col1, col2]:
        if column not in df.columns:
            raise KeyError(f"Column '{column}' not found in DataFrame")
        
    unique_group1_values = df[group1].unique() # group1 should be 'New Item Description'
    correlation_list = []

    for group1_value in unique_group1_values:
        group1_df = df[df[group1] == group1_value]
        unique_group2_values = group1_df[group2].unique() # group2 should be 'Doctor Name'

        for group2_value in unique_group2_values:
            group2_df = group1_df[group1_df[group2] == group2_value]
            unique_group3_values = group2_df[group3].unique() # group3 should be the third grouping

            for group3_value in unique_group3_values:
                subgroup_df = group2_df[group2_df[group3] == group3_value]
                if subgroup_df[[col1, col2]].dropna().shape[0] > 1:  # Ensure at least two non-NA rows
                    corr_value = subgroup_df[[col1, col2]].corr().iloc[0, 1]
                else:
                    corr_value = np.nan  # Not enough data to calculate correlation
                subgroup_count = len(subgroup_df)
                correlation_list.append((group1_value, group2_value, group3_value, subgroup_count, corr_value))

    correlation_df = pd.DataFrame(correlation_list, columns=[group1, group2, group3, 'Count', 'Correlation'])

    # Add Transactions and Overall count columns
    overall_count = len(df)
    correlation_df['Transactions'] = correlation_df['Count']
    correlation_df['Overall_Count'] = overall_count
    correlation_df['Correlation (r)'] = correlation_df['Correlation']
    correlation_df['Square Correlation (r^2)'] = correlation_df['Correlation'] ** 2
    correlation_df['Correlation*100 (r*100)'] = correlation_df['Correlation'] * 100

    return correlation_df



def save_df_to_excel_by_site(df, group_column, output_path):
    """
    Save a DataFrame to an Excel file with each group in a separate sheet.

    :param df: DataFrame to be saved
    :param group_column: Column name to group by (e.g., 'Hospital Site')
    :param output_path: Path to save the Excel file
    """
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        # Group the DataFrame by the specified column
        for group_name, group_df in df.groupby(group_column):
            # Write each group to a separate sheet named after the group
            group_df.to_excel(writer, sheet_name=str(group_name), index=False)
    # print(f"Data has been saved to {output_path}")
    




# #     print(f"All correlations have been saved to {output_file_path}")
# def save_all_correlations_to_excel_01(df, subgroups, nested_groups, col1, col2, output_file_path):
#     with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
#         overall_count = len(df)
#         unique_patient_count = df['HN'].nunique()

#         # Save the regular subgroup correlations with added counts
#         for subgroup in subgroups:
#             subgroup_correlations = calculate_subgroup_correlations(df, subgroup, col1, col2)
#             # Calculate the number of unique patients for each subgroup
#             subgroup_patient_counts = df.groupby(subgroup)['HN'].nunique().reset_index(name='Patients')
#             # Merge the counts with the correlations
#             subgroup_correlations = subgroup_correlations.merge(subgroup_patient_counts, on=subgroup, how='left')
#             # Add Transactions and Overall count columns
#             subgroup_correlations['Transactions'] = subgroup_correlations['Count']
#             subgroup_correlations['Overall_Count'] = overall_count

#             # The columns to keep should match the columns actually in subgroup_correlations
#             columns_to_keep = [subgroup, 'Patients', 'Transactions', 'Overall_Count', 'Correlation']
#             subgroup_correlations = subgroup_correlations[columns_to_keep]
#             # When saving to Excel, use the first 31 characters of the subgroup for the sheet name
#             sheet_name = subgroup[:31] if len(subgroup) > 31 else subgroup
#             subgroup_correlations.to_excel(writer, sheet_name=sheet_name, index=False)

#         # Save the category correlation charts data
#         category_correlation_data = get_category_correlation_data(df, subgroups, col1, col2)
#         category_correlation_data.to_excel(writer, sheet_name='Category_Correlations', index=False)

#         # Calculate and save the nested subgroup correlations
#         nested_correlations = calculate_nested_subgroup_correlations01(df, nested_groups[0], nested_groups[1], col1, col2)
#         # Add Patients column for the nested subgroups
#         for group in nested_groups:
#             if group == 'Doctor Name':  # This assumes that doctor counts are only relevant when 'Doctor Name' is part of the nesting
#                 doctor_patient_counts = calculate_doctor_hn_counts(df)
#                 nested_correlations = nested_correlations.merge(doctor_patient_counts, on=group, how='left')
#         nested_correlations.to_excel(writer, sheet_name='Nested_Correlations', index=False)

#     print(f"All correlations have been saved to {output_file_path}")



# def calculate_nested_subgroup_correlations01(df, group1, group2, col1, col2):
#     # First group by 'New Item Description' instead of 'Doctor Name'
#     unique_group1_values = df[group1].unique() # group1 should be 'New Item Description'
#     correlation_list = []

#     for group1_value in unique_group1_values:
#         group1_df = df[df[group1] == group1_value]
#         # Then group by 'Doctor Name'
#         unique_group2_values = group1_df[group2].unique() # group2 should be 'Doctor Name'

#         # ... rest of the code ...

#         for group2_value in unique_group2_values:
#             subgroup_df = group1_df[group1_df[group2] == group2_value]
#             if subgroup_df[[col1, col2]].dropna().shape[0] > 1:  # Ensure at least two non-NA rows
#                 corr_value = subgroup_df[[col1, col2]].corr().iloc[0, 1]
#             else:
#                 corr_value = np.nan  # Not enough data to calculate correlation
#             subgroup_count = len(subgroup_df)
#             correlation_list.append((group1_value, group2_value, subgroup_count, corr_value))

#     correlation_df = pd.DataFrame(correlation_list, columns=[group1, group2, 'Count', 'Correlation'])
#     return correlation_df








def export_custom_aggregated_data(df, output_file_path):
    # Create a new column for unique patient identifier
    df['Patient_Identifier'] = df['Hospital Site'] + '_' + df['HN']

    # Aggregate data
    agg_df = df.groupby(['Hospital Site', 'Item Description']).agg({
        'Patient_Identifier': pd.Series.nunique,
        'Revenue': 'sum',
        'Med_Days': 'sum',
        'Med_Qty': 'sum',
        'HN': 'size'  # Counting the number of rows for each group
    }).reset_index()

    # Calculate Revenue per Med_Days and Med_Qty
    agg_df['Revenue/Med_Days'] = agg_df['Revenue'] / agg_df['Med_Days'].replace(0, np.nan)
    agg_df['Revenue/Med_Qty'] = agg_df['Revenue'] / agg_df['Med_Qty'].replace(0, np.nan)

    # Rename columns for clarity
    agg_df.rename(columns={'Patient_Identifier': 'Patient Count', 'HN': 'Transaction Count'}, inplace=True)

    # Select and reorder columns
    final_df = agg_df[['Hospital Site', 'Item Code', 'Patient Count', 'Transaction Count', 'Revenue/Med_Days', 'Revenue/Med_Qty']]

    # Export to Excel
    final_df.to_excel(output_file_path, index=False)
    print(f"Data exported to '{output_file_path}'")


def export_custom_aggregated_data_v2(df, output_file_path):
    # Create a new column for unique patient identifier
    df['Patient_Identifier'] = df['Hospital Site'] + '_' + df['HN']

    # Aggregate data
    agg_df = df.groupby(['Hospital Site', 'CaseVisit_Appt']).agg({
        'Patient_Identifier': pd.Series.nunique,
        'HN': 'size'  # Counting the number of rows for each group
    }).reset_index()

    # Rename columns for clarity
    agg_df.rename(columns={
        'Patient_Identifier': 'Patient Count', 
        'HN': 'Transaction Count'
    }, inplace=True)

    # Select and reorder columns
    final_df = agg_df[['Hospital Site', 'CaseVisit_Appt', 'Patient Count', 'Transaction Count']]

    # Export to Excel
    final_df.to_excel(output_file_path, index=False)
    print(f"Data exported to '{output_file_path}'")
    
    return final_df





