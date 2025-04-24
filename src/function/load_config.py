import json
from pathlib import Path

def load_all_config(main_config_path="config/main_config.json"):
    """Load main config and all referenced subconfigs."""
    main_config_path = Path(main_config_path)

    with open(main_config_path, encoding="utf-8") as f:
        main_config = json.load(f)

    def load_sub(path_key):
        sub_path = Path(main_config["configs"][path_key])
        with open(sub_path, encoding="utf-8") as f:
            return json.load(f)

    return {
        "main": main_config,
        "data_paths": load_sub("data_paths"),
        "column_renames": load_sub("column_renames"),
        "filters": load_sub("filters"),
        "clinic_code_map": load_sub("clinic_mappings"),
    }

# config = load_all_config()

# # Access as needed:
# input_path = config["data_paths"]["processed"]["item_type_drug"]["PT1"]
# rename_map = config["column_renames"]["item_type_drug"]
# excluded_doctors = config["filters"]["excluded_doctors"]
