import os


# Function to find CSV file
def check_csv_file(possible_paths):
    for path in possible_paths:
        if os.path.exists(path):
            print(f"Using alerts conditions from: {path}")
            return path

    print("Warning: No alerts_conditions.csv found in any expected location!")
    return None
