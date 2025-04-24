#%%
import sys
import time

# Adding the parent directory to sys.path for module imports
sys.path.append('../')

import pandas as pd
import json
from function.import_data import *
from function.parseThaiDate import *
from function.exportExcel import *
from function.combine import *

start_time = time.time()

# Load configuration
config_path = "/Users/jinjuthatedcharoen/Documents/PPG/P'Aim/Inventory/Python/src/config.json"
with open(config_path, 'r') as file:
    config = json.load(file)
    

path = "../../../Data/Result/clean_data/Combined/combined_all.parquet"

df = load_parquet(path)
# Group by BU and count the number of transactions for each BU
bu_counts = df['BU'].value_counts().sort_index()
total = bu_counts.sum()

# Create a DataFrame for the table
table_df = pd.DataFrame({
    "BU": bu_counts.index,
    "Count": bu_counts.values,
    "Percentage": (bu_counts.values / total * 100).round(1)
})

# Append a total row to the table DataFrame
total_row = pd.DataFrame({
    "BU": ["Total"],
    "Count": [total],
    "Percentage": [100.0]
})
table_df = pd.concat([table_df, total_row], ignore_index=True)

# Create the bar chart
plt.figure(figsize=(10, 6))
bars = plt.bar(bu_counts.index, bu_counts.values)

plt.xlabel("BU")
plt.ylabel("Count of Transactions")
plt.title("BU vs Count of Transactions")
plt.xticks(rotation=45)

# Add labels on top of each bar (count and percentage)
for bar in bars:
    height = bar.get_height()
    percent = (height / total) * 100
    label = f'{int(height)} ({percent:.1f}%)'
    plt.text(
        bar.get_x() + bar.get_width() / 2,
        height,
        label,
        ha='center',
        va='bottom'
    )

# Add a table below the bar chart
the_table = plt.table(
    cellText=table_df.values,
    colLabels=table_df.columns,
    cellLoc='center',
    loc='bottom',
    bbox=[0, -1.6, 1, 1]  # [left, bottom, width, height] in figure coordinates
)

# Change text color for all cells in the table to black
for key, cell in the_table.get_celld().items():
    cell.get_text().set_color("black")

plt.subplots_adjust(left=0.2, bottom=0.3)  # Adjust the subplot to make room for the table
plt.tight_layout()
plt.show()

# %%
