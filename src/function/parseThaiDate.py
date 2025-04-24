import os
import glob
import chardet
import pandas as pd
import shutil
import xlrd

# --------------------------------------------------------------------
# 1) Thai month abbreviations mapping
# --------------------------------------------------------------------
thai_months = {
    "ม.ค.": "01",
    "ก.พ.": "02",
    "มี.ค.": "03",
    "เม.ย.": "04",
    "พ.ค.": "05",
    "มิ.ย.": "06",
    "ก.ค.": "07",
    "ส.ค.": "08",
    "ก.ย.": "09",
    "ต.ค.": "10",
    "พ.ย.": "11",
    "ธ.ค.": "12"
}

# --------------------------------------------------------------------
# 2) Function to parse Thai-style dates (e.g. "20-มี.ค.-24")
# --------------------------------------------------------------------
def parse_thai_date(date_str):
    """
    Parse Thai-style dates like '20-มี.ค.-24' into a proper datetime.
    Adjust logic if you use B.E. (e.g., '2567' -> 2024).
    """
    parts = date_str.split("-")
    if len(parts) != 3:
        return pd.NaT
    
    day_str, thai_month, year_str = [p.strip() for p in parts]
    
    # Convert 2-digit years (24 -> 2024). If you have B.E. years, subtract 543.
    try:
        year = int(year_str)
        if year < 100:
            year += 2000
    except ValueError:
        return pd.NaT
    
    # Convert Thai month abbreviation to numeric month
    month = thai_months.get(thai_month, None)
    if not month:
        return pd.NaT
    
    # Convert day to int
    try:
        day = int(day_str)
    except ValueError:
        return pd.NaT
    
    # Build an ISO string and parse
    iso_string = f"{year:04d}-{month}-{day:02d}"
    return pd.to_datetime(iso_string, format="%Y-%m-%d", errors="coerce")

# --------------------------------------------------------------------
# 3) Function to convert a single .xls (text) file to .xlsx
# --------------------------------------------------------------------

def convert_thai_file_to_xlsx(input_file, output_folder, skiprows: int = 0):
    import os, chardet, pandas as pd

    # Detect encoding
    with open(input_file, 'rb') as f:
        sample = f.read(100_000)
    detected = chardet.detect(sample)
    encoding = detected['encoding'] or 'cp874'
    print(f"File: {input_file}\n  Detected encoding: {encoding} ({detected['confidence']*100:.1f}%)")

    # Try multiple encodings with safe error handling
    for enc in (encoding, 'cp874', 'utf-8'):
        try:
            df = pd.read_csv(
                input_file,
                sep="\t",
                encoding=enc,
                encoding_errors='replace',
                skiprows=skiprows,
                engine='python',
                on_bad_lines='skip'
            )
            print(f"  → Successfully read with encoding: {enc}")
            break
        except Exception as e:
            print(f"    ✗ Failed to read with {enc}: {e}")
    else:
        raise RuntimeError("All encoding attempts failed.")
    
    # Split single‑column pipe‑delimited text into real columns
    if df.shape[1] == 1 and df.columns[0].find("|") != -1:
        split_df = df.iloc[:, 0].str.split("|", expand=True)
        split_df.columns = split_df.iloc[0]        # first row becomes header
        df = split_df.drop(index=0).reset_index(drop=True)

    # Parse Thai date if present
    if "TRANSACTION_DATE" in df.columns:
        df["TRANSACTION_DATE"] = df["TRANSACTION_DATE"].apply(parse_thai_date)

    # Save to XLSX
    base = os.path.splitext(os.path.basename(input_file))[0]
    out_path = os.path.join(output_folder, f"{base}.xlsx")
    df.to_excel(out_path, index=False)
    print(f"  Saved → {out_path}\n")


# def convert_pipe_delimited_file_to_xlsx(input_file, output_folder, skiprows: int = 0):

#     # 1️⃣ Detect encoding
#     with open(input_file, 'rb') as f:
#         sample = f.read(100_000)
#     result = chardet.detect(sample)
#     detected = result.get('encoding') or 'cp874'
#     print(f"Processing {input_file}\n  Detected encoding: {detected} ({result['confidence']*100:.1f}%)")

#     # Override SHIFT_JIS → cp874
#     encodings = ['cp874', 'utf-8'] if detected.lower() == 'shift_jis' else [detected, 'cp874', 'utf-8']

#     df = None
#     for enc in dict.fromkeys(encodings):
#         try:
#             df = pd.read_csv(
#                 input_file,
#                 sep="|",
#                 encoding=enc,
#                 engine="python",
#                 on_bad_lines="skip",
#                 skiprows=skiprows
#             )
#             print(f"  → Successfully read with encoding: {enc}")
#             break
#         except Exception as e:
#             print(f"    ✗ Failed with {enc}: {e}")
#     if df is None:
#         raise RuntimeError("All encoding attempts failed.")

#     # Split single-column edge‑case
#     if df.shape[1] == 1 and '|' in df.columns[0]:
#         df = df.iloc[:, 0].str.split('|', expand=True)
#         df.columns = df.iloc[0]
#         df = df.drop(0).reset_index(drop=True)

#     # ----- TYPE CONVERSIONS -----
#     numeric_cols = ['SUBINVENTORY_CODE', 'STORE_CODE', 'SubInventory']
#     for col in numeric_cols:
#         if col in df.columns:
#             df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

#     # date_cols = ['TRANSACTION_DATE', 'EXPIRE_DATE', 'Invoice Date', 'Receipt Date', 'วันหมดอายุ']
#     # for col in date_cols:
#     #     if col in df.columns:
#     #         df[col] = pd.to_datetime(df[col], format='%d-%b-%y', dayfirst=True, errors='coerce')
    
#     date_cols = ['TRANSACTION_DATE', 'EXPIRE_DATE', 'Invoice Date', 'Receipt Date', 'วันหมดอายุ', 'EXPIRE_DATE', 'REPORTED', 'CREATION_DATE']
#     for col in date_cols:
#         if col in df.columns:
#             df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')

#     # ----- FORCE ID COLUMNS TO TEXT -----
#     for col in ['Recept No', 'PO No', 'Invoice No']:
#         if col in df.columns:
#             df[col] = df[col].astype(str)

#     # 4️⃣ Write out to XLSX with date formatting
#     base = os.path.splitext(os.path.basename(input_file))[0]
#     output_path = os.path.join(output_folder, f"{base}.xlsx")
        
#     with pd.ExcelWriter(output_path, engine='xlsxwriter',
#                         date_format='dd-mmm-yy', datetime_format='dd-mmm-yy') as writer:
#         df.to_excel(writer, index=False)
#         worksheet = writer.sheets['Sheet1']
#         worksheet.ignore_errors({'type': 'number_stored_as_text'})
#         for idx, col in enumerate(df.columns):
#             worksheet.set_column(idx, idx, len(col) + 2)

#     print(f"Saved → {output_path}")


def convert_pipe_delimited_file_to_xlsx(input_file, output_folder, skiprows: int = 0):
    base = os.path.splitext(os.path.basename(input_file))[0]
    ext = os.path.splitext(input_file)[1].lower()

    os.makedirs(output_folder, exist_ok=True)

    df = None

    # 2️⃣ Try reading .xls — if it errors, just continue into the pipe‑delimited branch
    if ext.lower() == ".xls":
        try:
            print("Processing Excel (.xls) → converting to .xlsx")
            df = pd.read_excel(input_file, skiprows=skiprows)
        except ValueError:
            df = None

    # 3️⃣ If not .xls or .xls read failed, do pipe‑delimited parsing
    if df is None:
        with open(input_file, 'rb') as f:
            sample = f.read(100_000)
        result = chardet.detect(sample)
        enc = result.get('encoding') or 'cp874'
        print(f"Processing {input_file}\n  Detected encoding: {enc} ({result['confidence']*100:.1f}%)")

        encodings = ['cp874', 'utf-8'] if enc.lower() == 'shift_jis' else [enc, 'cp874', 'utf-8']
        for e in dict.fromkeys(encodings):
            try:
                df = pd.read_csv(
                    input_file,
                    sep="|",
                    encoding=e,
                    engine="python",
                    on_bad_lines="skip",
                    skiprows=skiprows
                )
                print(f"  → Successfully read with encoding: {e}")
                break
            except Exception:
                continue

        if df is None:
            raise RuntimeError("All encoding attempts failed.")

        # Split single‑column edge case
        if df.shape[1] == 1 and "|" in df.columns[0]:
            df = df.iloc[:, 0].str.split("|", expand=True)
            df.columns = df.iloc[0]
            df = df.drop(0).reset_index(drop=True)

    # 4️⃣ Type conversions
    for col in ['SUBINVENTORY_CODE', 'STORE_CODE', 'SubInventory']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    for col in ['TRANSACTION_DATE', 'EXPIRE_DATE', 'Invoice Date', 'Receipt Date', 'วันหมดอายุ', 'REPORTED', 'CREATION_DATE']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
    for col in ['Recept No', 'PO No', 'Invoice No']:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # 5️⃣ Write to .xlsx
    output_path = os.path.join(output_folder, f"{base}.xlsx")
    with pd.ExcelWriter(output_path, engine='xlsxwriter',
                        date_format='dd-mmm-yy', datetime_format='dd-mmm-yy') as writer:
        df.to_excel(writer, index=False)
        ws = writer.sheets['Sheet1']
        ws.ignore_errors({'type': 'number_stored_as_text'})
        for i, col in enumerate(df.columns):
            ws.set_column(i, i, len(col) + 2)

    print(f"Saved → {output_path}")


# def convert_orcmii_file_to_xlsx(input_file, output_folder, skiprows: int = 0):
#     base = os.path.splitext(os.path.basename(input_file))[0]
#     ext = os.path.splitext(input_file)[1]
#     # If it’s already an .xlsx, just copy it over
#     if ext.lower() == ".xlsx":
#         os.makedirs(output_folder, exist_ok=True)
#         output_path = os.path.join(output_folder, os.path.basename(input_file))
#         shutil.copy2(input_file, output_path)
#         print(f"Skipped conversion — copied existing Excel → {output_path}")
#         return
    
#     # For .xls files fall through into conversion logic (read via read_excel)
#     if ext == ".xls":
#         print(f"Processing Excel (.xls) → converting to .xlsx")
#         df = pd.read_excel(input_file, skiprows=skiprows)
#     else:
#         # Detect encoding
#         with open(input_file, 'rb') as f:
#             sample = f.read(100_000)
#         detected = chardet.detect(sample)
#         encoding = detected.get('encoding') or 'cp874'
#         # Force an encoding if needed:
#         if encoding.lower() == 'ascii':
#             encoding = 'cp874'  # or 'utf-8' or 'tis-620', based on your file's actual encoding
#         print(f"Processing {input_file}\n  Detected encoding: {encoding} ({detected['confidence']*100:.1f}%)")

#         # Read entire tab‑delimited file
#         df = pd.read_csv(
#             input_file,
#             sep="\t",
#             encoding=encoding, # Could be "utf-8", "cp874", "tis-620", etc.
#             skiprows=skiprows,
#             engine="python",
#             on_bad_lines="skip"
#         )

#     # Identify start indices of each repeated block (header == "Item")
#     cols = df.columns.tolist()
#     starts = [i for i, name in enumerate(cols) if name.strip().lower() == "item"]
#     ends = starts[1:] + [len(cols)]

#     # Write each block into its own sheet
#     # base = os.path.splitext(os.path.basename(input_file))[0]
#     # output_path = os.path.join(output_folder, f"{base}.xlsx")
    
#     os.makedirs(output_folder, exist_ok=True)
#     output_path = os.path.join(output_folder, f"{base}.xlsx")
    
#     with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
#         for idx, (start, end) in enumerate(zip(starts, ends), start=1):
#             block = df.iloc[:, start:end].copy()
#             # Drop completely empty rows
#             block.dropna(how="all", inplace=True)
#             sheet_name = f"Sheet{idx}"
#             block.to_excel(writer, sheet_name=sheet_name, index=False)
#             print(f"  → Wrote {sheet_name} ({block.shape[0]} rows × {block.shape[1]} cols)")

#     print(f"Saved → {output_path}")

def convert_orcmii_file_to_xlsx(input_file, output_folder, skiprows: int = 0):
    base = os.path.splitext(os.path.basename(input_file))[0]
    ext = os.path.splitext(input_file)[1].lower()
    # Skip existing .xlsx
    if ext.lower() == ".xlsx":
        os.makedirs(output_folder, exist_ok=True)
        output_path = os.path.join(output_folder, os.path.basename(input_file))
        shutil.copy2(input_file, output_path)
        print(f"Skipped conversion — copied existing Excel → {output_path}")
        return

    # Convert .xls via read_excel
    # if ext.lower() == ".xls":
    #     print(f"Processing Excel (.xls) → converting to .xlsx")
    #     df = pd.read_excel(input_file, skiprows=skiprows)
    if ext.lower() == ".xls":
        print(f"Processing Excel (.xls) → converting to .xlsx")
        try:
            df = pd.read_excel(input_file, skiprows=skiprows, engine="xlrd")
        except Exception as e:
            print(f"⚠️ read_excel failed: {e}\n   Falling back to tab‑delimited parser")

            with open(input_file, "rb") as f:
                sample = f.read(100_000)
            detected = chardet.detect(sample)
            encoding = detected.get("encoding") or "cp874"
            if encoding.lower() == "ascii":
                encoding = "cp874"
            print(f"Processing {input_file}\n  Detected encoding: {encoding} ({detected['confidence']*100:.1f}%)")

            df = pd.read_csv(
                input_file,
                sep="\t",
                encoding=encoding,
                skiprows=skiprows,
                engine="python",
                on_bad_lines="skip"
            )
    else:
        # Parse tab‑delimited text
        with open(input_file, "rb") as f:
            sample = f.read(100_000)
        detected = chardet.detect(sample)
        encoding = detected.get("encoding") or "cp874"
        if encoding.lower() == "ascii":
            encoding = "cp874"
        print(f"Processing {input_file}\n  Detected encoding: {encoding} ({detected['confidence']*100:.1f}%)")

        df = pd.read_csv(
            input_file,
            sep="\t",
            encoding=encoding,
            skiprows=skiprows,
            engine="python",
            on_bad_lines="skip"
        )

    # Split into blocks by “Item” header
    cols = df.columns.tolist()
    starts = [i for i, c in enumerate(cols) if c.strip().lower() == "item"]
    ends = starts[1:] + [len(cols)]

    os.makedirs(output_folder, exist_ok=True)
    output_path = os.path.join(output_folder, f"{base}.xlsx")

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        for idx, (start, end) in enumerate(zip(starts, ends), start=1):
            block = df.iloc[:, start:end].dropna(how="all")
            sheet_name = f"Sheet{idx}"
            block.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"  → Wrote {sheet_name} ({block.shape[0]} rows × {block.shape[1]} cols)")

    print(f"Saved → {output_path}")