import json

# Function to load data from an config.json path
def load_config(config_path):
    """Load configuration from a JSON file."""
    try:
        with open(config_path, 'r') as file:
            config = json.load(file)
        # print("Loaded configuration:", {config_path})
        return config
    except FileNotFoundError:
        print(f"Configuration file not found at {config_path}")
        return None
    except json.JSONDecodeError:
        print(f"Error decoding JSON from the configuration file at {config_path}")
        return None