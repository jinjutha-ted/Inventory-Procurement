# 💊 Inventory-Procurement Automation Project

This project automates monthly data pipelines for drug prescription analytics across multiple hospital sites. It standardizes file paths, cleans and combines raw inputs, applies site-specific transformation rules, and exports outputs for reporting.

---

## 📋 Table of Contents

- [Project Structure](#project-structure)  
- [Configuration](#configuration)  
- [Setup & Installation](#setup--installation)  
- [How to Run](#how-to-run)  
- [Folder Guide](#folder-guide)  
- [Future Improvements](#future-improvements)  
- [License](#license)  

---

## 📁 Project Structure

NVENTORY/ ├── .venv/ ← Python virtual environment ├── config/ ← All modular JSON configuration files │ ├── main_config.json │ ├── data_path.json │ ├── column_rename.json │ ├── filter.json │ └── clinic.json ├── data/ │ ├── raw/ ← Monthly input files │ ├── processed/ ← Intermediate cleaned data │ └── output/ ← Final exports (Excel, Parquet) ├── database/ ← SQL connection files & scripts ├── src/ │ ├── function/ ← Reusable logic (cleaning, parsing, exports) │ ├── monthly_run/ ← Main scripts for monthly automation │ └── Inv/ ← Site-specific or exploratory scripts ├── requirements.txt ← Python dependencies └── README.md ← This file

---

## ⚙️ Configuration

All settings are centralized under the `config/` folder. This modular setup keeps paths, renaming, filters, and master mappings organized:

| File | Description |
|------|-------------|
| `main_config.json`     | Primary file that links all subconfigs |
| `data_path.json`       | Paths to raw, processed, and output files |
| `column_rename.json`   | Rename logic for different data types or sites |
| `filter.json`          | Filter rules (e.g., drop rows, exclude doctors) |
| `clinic.json`          | Master clinic code mappings |

### 🔁 Load config in Python

```python
from function.load_config import load_all_config

config = load_all_config()

### Example usage
raw_path = config["data_paths"]["processed"]["item_type_drug"]["PT1"]
rename_rules = config["column_renames"]["item_type_drug"]

### 🚀 Download

Clone this repository:

```bash
git clone https://github.com/jinjutha-ted/Inventory-Procurement.git

```bash
cd Inventory-Procurement
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

###🚀 How to Run

python src/monthly_run/main.py


Folder | Purpose
config/ | All configuration files used by the pipeline
data/raw/ | Raw input files organized by month
data/processed/ | Cleaned and combined intermediate results
data/output/ | Final reports for analytics
src/function/ | All reusable transformation logic
src/monthly_run/ | Entry scripts for running the full pipeline
database/ | DB connection and SQL logic (if used)
Inv/ | One-off or exploratory scripts (e.g., site-specific work)