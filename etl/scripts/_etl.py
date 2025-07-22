import pandas as pd
import os
import re

# Define base directories
# Script is in etl/scripts/, so BASE_DIR should be the etl directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCE_DIR = os.path.join(BASE_DIR, 'source')
# Go up one more level to get to the root directory for output
ROOT_DIR = os.path.dirname(BASE_DIR)
DATAPOINTS_DIR = os.path.join(ROOT_DIR, 'datapoints')
ENTITIES_DIR = os.path.join(ROOT_DIR, 'ddf--entities--geo.csv')
CONCEPTS_DIR = os.path.join(ROOT_DIR, 'ddf--concepts.csv')

# Define source file names
FRA_YEARS_FILE = 'FRA_Years_2025_07_14.csv'
ANNUAL_FILE = 'Annual_2025_07_14.csv'
VARIABLE_DEFINITIONS_FILE = 'variable_definitions.csv'

def clean_concept_id(concept_name):
    """
    Cleans concept IDs by removing # symbols and converting to snake_case.
    """
    # Replace # with 'num' to avoid invalid concept IDs
    concept_name = concept_name.replace('#', 'num')

    # Convert to snake_case
    concept_name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', concept_name)
    concept_name = re.sub('__([A-Z])', r'_\1', concept_name)
    concept_name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', concept_name)
    return concept_name.lower()

def create_geo_entities(source_files):
    """
    Creates geo entities file by extracting unique iso3 codes and names from source files.
    """
    print("Creating geo entities...")

    geo_data = []

    for filepath in source_files:
        if not os.path.exists(filepath):
            print(f"Source file not found: {filepath}. Skipping for geo entities.")
            continue

        try:
            df = pd.read_csv(filepath, na_values=['', ' ', 'NaN', 'N/A'])

            # Extract geo information
            if 'iso3' in df.columns and 'name' in df.columns:
                geo_subset = df[['iso3', 'name']].drop_duplicates()
                geo_data.append(geo_subset)

        except Exception as e:
            print(f"Error reading {filepath} for geo entities: {e}")
            continue

    if geo_data:
        # Combine all geo data and remove duplicates
        combined_geo = pd.concat(geo_data, ignore_index=True).drop_duplicates()

        # Clean and format
        combined_geo['geo'] = combined_geo['iso3'].astype(str).str.lower()
        combined_geo['name'] = combined_geo['name'].astype(str)

        # Create final geo entities dataframe
        geo_entities = combined_geo[['geo', 'name']].copy()
        geo_entities = geo_entities.sort_values('geo').reset_index(drop=True)

        # Save geo entities file
        geo_entities.to_csv(ENTITIES_DIR, index=False)
        print(f"Created geo entities file: {ENTITIES_DIR}")
        print(f"Total geo entities: {len(geo_entities)}")
    else:
        print("No geo data found to create entities file.")

def create_concepts_file():
    """
    Creates concepts file from variable definitions.
    """
    print("Creating concepts file...")

    variable_def_path = os.path.join(SOURCE_DIR, VARIABLE_DEFINITIONS_FILE)

    if not os.path.exists(variable_def_path):
        print(f"Variable definitions file not found: {variable_def_path}")
        return

    try:
        # First, let's identify problematic lines
        print("Checking for problematic lines in variable definitions CSV...")
        with open(variable_def_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        for i, line in enumerate(lines, 1):
            line = line.strip()
            if not line:
                continue
            # Count commas outside quotes to check field count
            comma_count = 0
            in_quotes = False
            for char in line:
                if char == '"':
                    in_quotes = not in_quotes
                elif char == ',' and not in_quotes:
                    comma_count += 1

            expected_commas = 2  # For 3 fields
            if comma_count != expected_commas:
                print(f"BAD LINE {i} (has {comma_count + 1} fields, expected 3): {line}")

        # Try to read the CSV normally
        var_df = pd.read_csv(variable_def_path, quotechar='"', skipinitialspace=True)

        # Clean concept IDs
        var_df['concept'] = var_df['variable_code'].apply(clean_concept_id)

        # Rename columns to match DDF concepts format
        concepts_df = var_df[['concept', 'variable_name', 'description']].copy()
        concepts_df.columns = ['concept', 'name', 'description']

        # Add concept_type and domain columns
        concepts_df['concept_type'] = 'measure'
        concepts_df['domain'] = 'geo'  # All measures apply to geo entities

        # Add standard DDF concepts
        standard_concepts = pd.DataFrame([
            {'concept': 'geo', 'name': 'Geographic Entity', 'description': 'Geographic entity identifier', 'concept_type': 'entity_domain', 'domain': ''},
            {'concept': 'year', 'name': 'Year', 'description': 'Year', 'concept_type': 'time', 'domain': ''},
            {'concept': 'name', 'name': 'Name', 'description': 'Name of the entity', 'concept_type': 'string', 'domain': ''},
            {'concept': 'domain', 'name': 'Domain', 'description': 'Entity set domain for the concept', 'concept_type': 'string', 'domain': ''},
            {'concept': 'description', 'name': 'Description', 'description': 'Description', 'concept_type': 'string', 'domain': ''},
        ])

        # Combine all concepts
        all_concepts = pd.concat([standard_concepts, concepts_df], ignore_index=True)
        all_concepts = all_concepts.drop_duplicates(subset=['concept']).reset_index(drop=True)

        # Save concepts file
        all_concepts.to_csv(CONCEPTS_DIR, index=False)
        print(f"Created concepts file: {CONCEPTS_DIR}")
        print(f"Total concepts: {len(all_concepts)}")

    except Exception as e:
        print(f"Error creating concepts file: {e}")

def process_file(filepath, output_dir):
    """
    Reads a source CSV file and creates a DDF datapoint CSV for each variable column.
    """
    if not os.path.exists(filepath):
        print(f"Source file not found: {filepath}. Skipping.")
        return

    print(f"Processing {filepath}...")

    try:
        df = pd.read_csv(filepath, na_values=['', ' ', 'NaN', 'N/A'])
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return

    # Define primary key columns and descriptive columns that are not variables
    id_vars = ['iso3', 'year']
    descriptive_vars = ['regions', 'deskStudy', 'name']

    # Check if the essential id_vars exist in the dataframe
    if not all(col in df.columns for col in id_vars):
        print(f"Skipping {filepath}: does not contain required columns 'iso3' and 'year'.")
        return

    # Identify variable columns (i.e., not primary keys or descriptive columns)
    variable_cols = [col for col in df.columns if col not in id_vars + descriptive_vars]

    for var_original in variable_cols:
        # Create a clean concept ID
        var_clean = clean_concept_id(var_original)

        print(f"  Generating datapoint for variable: {var_original} -> {var_clean}")

        # Create a new DataFrame for the specific datapoint
        datapoint_df = df[id_vars + [var_original]].copy()

        # Rename 'iso3' to 'geo' as requested
        datapoint_df.rename(columns={'iso3': 'geo'}, inplace=True)

        # Drop rows where the variable's value is missing
        datapoint_df.dropna(subset=[var_original], inplace=True)

        # Skip if no data is left after dropping missing values
        if datapoint_df.empty:
            print(f"  Skipping empty datapoint file for {var_original}")
            continue

        # Rename the variable column to its clean name
        datapoint_df.rename(columns={var_original: var_clean}, inplace=True)

        # Clean and set data types
        datapoint_df['year'] = pd.to_numeric(datapoint_df['year'], errors='coerce').astype('Int64')
        datapoint_df['geo'] = datapoint_df['geo'].astype(str).str.lower()

        # Attempt to convert variable column to a numeric type
        datapoint_df[var_clean] = pd.to_numeric(datapoint_df[var_clean], errors='coerce')

        # Drop rows that could not be converted to a number if the column was meant to be numeric
        datapoint_df.dropna(subset=[var_clean], inplace=True)

        # Final check if dataframe has content before saving
        if datapoint_df.empty:
            print(f"  Skipping empty datapoint file for {var_original} after data cleaning.")
            continue

        # Define the output path and filename according to DDF conventions
        output_filename = f'ddf--datapoint--{var_clean}--by--geo--year.csv'
        output_path = os.path.join(output_dir, output_filename)

        # Save the processed data to a new CSV file
        datapoint_df.to_csv(output_path, index=False)

def main():
    """Main function to run the ETL process."""
    print("Starting ETL process...")

    # Ensure the output directory exists
    if not os.path.exists(DATAPOINTS_DIR):
        os.makedirs(DATAPOINTS_DIR)
        print(f"Created output directory: {DATAPOINTS_DIR}")

    # Define source files
    source_files = [
        os.path.join(SOURCE_DIR, FRA_YEARS_FILE),
        os.path.join(SOURCE_DIR, ANNUAL_FILE)
    ]

    # Create geo entities file
    create_geo_entities(source_files)

    # Create concepts file
    create_concepts_file()

    # Process both specified files for datapoints
    for source_file in source_files:
        process_file(source_file, DATAPOINTS_DIR)

    print("ETL process completed.")

if __name__ == '__main__':
    main()
