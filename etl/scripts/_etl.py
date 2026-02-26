import glob
import os
import re

import pandas as pd

# Define base directories
# Script is in etl/scripts/, so BASE_DIR should be the etl directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCE_DIR = os.path.join(BASE_DIR, "source")
# Go up one more level to get to the root directory for output
ROOT_DIR = os.path.dirname(BASE_DIR)
DATAPOINTS_DIR = os.path.join(ROOT_DIR, "datapoints")
ENTITIES_DIR = os.path.join(ROOT_DIR, "ddf--entities--geo.csv")
CONCEPTS_DIR = os.path.join(ROOT_DIR, "ddf--concepts.csv")

VARIABLE_DEFINITIONS_FILE = "variable_definitions.csv"
TERMS_DEFINITIONS_FILE = "Terms and Definitions.md"

ID_VARS = ["iso3", "year"]
DESCRIPTIVE_VARS = ["regions", "deskStudy", "Desk study", "name", "iso2", "m49"]
CLIMATE_DOMAIN_COLS = ["boreal", "subtropical", "temperate", "tropical"]


def clean_concept_id(concept_name):
    """
    Cleans concept IDs by removing # symbols and converting to snake_case.
    """
    # Replace # with 'num' to avoid invalid concept IDs
    concept_name = concept_name.replace("#", "num")

    # Convert to snake_case
    concept_name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", concept_name)
    concept_name = re.sub("__([A-Z])", r"_\1", concept_name)
    concept_name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", concept_name)
    return concept_name.lower()


def humanize_concept_name(concept):
    """Create a fallback human-readable concept name from concept ID."""
    return concept.replace("_", " ").replace("num", "#").upper()


def get_latest_source_file(prefix):
    """
    Find the most recent source file by filename pattern.
    Expected examples: FRA_Years_2026-02-25.csv, Annual_2026-02-25.csv
    """
    pattern = os.path.join(SOURCE_DIR, f"{prefix}_*.csv")
    candidates = sorted(glob.glob(pattern))
    if not candidates:
        return None

    # Sort by extracted date-like token in filename if available, else mtime.
    def sort_key(path):
        name = os.path.basename(path)
        m = re.search(r"(\d{4}[-_]\d{2}[-_]\d{2}|\d{4}_\d{2}_\d{2})", name)
        if m:
            return (1, m.group(1).replace("_", "-"))
        return (0, f"{os.path.getmtime(path):020.3f}")

    return sorted(candidates, key=sort_key)[-1]


def get_source_files():
    """Resolve current FRA and Annual source files."""
    fra_years_file = get_latest_source_file("FRA_Years")
    annual_file = get_latest_source_file("Annual")

    source_files = [p for p in [fra_years_file, annual_file] if p]

    if not source_files:
        raise FileNotFoundError(
            f"No source files matching FRA_Years_*.csv or Annual_*.csv in {SOURCE_DIR}"
        )

    print("Using source files:")
    for p in source_files:
        print(f"  - {os.path.basename(p)}")

    return source_files


def load_terms_section_titles():
    """
    Parse section titles from Terms and Definitions file.
    Example heading: '## 1a Extent of forest and other wooded land'
    """
    terms_path = os.path.join(SOURCE_DIR, TERMS_DEFINITIONS_FILE)
    titles = {}

    if not os.path.exists(terms_path):
        return titles

    try:
        with open(terms_path, "r", encoding="utf-8-sig") as f:
            for line in f:
                line = line.strip()
                m = re.match(r"^##\s+(\d+[a-zA-Z]?)\s+(.+)$", line)
                if m:
                    code = m.group(1).lower()
                    title = m.group(2).strip()
                    titles[code] = title
    except Exception as e:
        print(f"Warning: failed reading {TERMS_DEFINITIONS_FILE}: {e}")

    return titles


def extract_variable_concepts(source_files):
    """Extract canonical concept IDs from source CSV columns."""
    concepts = set()

    for filepath in source_files:
        if not os.path.exists(filepath):
            continue

        df_head = pd.read_csv(filepath, nrows=0, encoding="utf-8-sig")
        cols = list(df_head.columns)

        # Remove known non-variable columns
        variable_cols = [
            col
            for col in cols
            if col not in ID_VARS + DESCRIPTIVE_VARS + CLIMATE_DOMAIN_COLS
        ]

        concepts.update(clean_concept_id(col) for col in variable_cols)

    return sorted(concepts)


def infer_concept_types(source_files):
    """
    Infer concept_type per source variable from observed values.
    Non-numeric concepts are emitted as string (e.g. yes/no fields).
    """
    concept_types = {}

    for filepath in source_files:
        if not os.path.exists(filepath):
            continue

        try:
            df = pd.read_csv(
                filepath,
                na_values=["", " ", "NaN", "N/A"],
                encoding="utf-8-sig",
                low_memory=False,
            )
        except Exception as e:
            print(f"Warning: failed reading {filepath} for concept type inference: {e}")
            continue

        variable_cols = [
            col
            for col in df.columns
            if col not in ID_VARS + DESCRIPTIVE_VARS + CLIMATE_DOMAIN_COLS
        ]

        for var_original in variable_cols:
            concept = clean_concept_id(var_original)
            if concept_types.get(concept) == "string":
                continue

            series = df[var_original].dropna()
            if series.empty:
                concept_types[concept] = concept_types.get(concept, "measure")
                continue

            numeric = pd.to_numeric(series, errors="coerce")
            if numeric.notna().sum() == len(series):
                concept_types[concept] = concept_types.get(concept, "measure")
            else:
                concept_types[concept] = "string"

    return concept_types


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
            df = pd.read_csv(
                filepath, na_values=["", " ", "NaN", "N/A"], encoding="utf-8-sig"
            )

            # Extract geo information
            if "iso3" in df.columns and "name" in df.columns:
                geo_subset = df[["iso3", "name"]].drop_duplicates()
                geo_data.append(geo_subset)

        except Exception as e:
            print(f"Error reading {filepath} for geo entities: {e}")
            continue

    if geo_data:
        # Combine all geo data and remove duplicates
        combined_geo = pd.concat(geo_data, ignore_index=True).drop_duplicates()

        # Clean and format
        combined_geo["geo"] = combined_geo["iso3"].astype(str).str.lower()
        combined_geo["name"] = combined_geo["name"].astype(str)

        # Create final geo entities dataframe
        geo_entities = combined_geo[["geo", "name"]].copy()
        geo_entities = geo_entities.sort_values("geo").reset_index(drop=True)

        # Save geo entities file
        geo_entities.to_csv(ENTITIES_DIR, index=False)
        print(f"Created geo entities file: {ENTITIES_DIR}")
        print(f"Total geo entities: {len(geo_entities)}")
    else:
        print("No geo data found to create entities file.")


def create_concepts_file(source_files):
    """
    Creates concepts file from source variables and optional metadata files.

    `variable_definitions.csv` is optional. If present, it enriches names/descriptions.
    If missing/outdated, concepts are still generated directly from source columns.
    """
    print("Creating concepts file...")

    source_concepts = extract_variable_concepts(source_files)
    concept_types = infer_concept_types(source_files)
    section_titles = load_terms_section_titles()

    # Optional metadata from variable_definitions.csv
    var_meta = {}
    variable_def_path = os.path.join(SOURCE_DIR, VARIABLE_DEFINITIONS_FILE)
    if os.path.exists(variable_def_path):
        try:
            var_df = pd.read_csv(
                variable_def_path,
                quotechar='"',
                skipinitialspace=True,
                encoding="utf-8-sig",
            )
            for _, row in var_df.iterrows():
                code = str(row.get("variable_code", "")).strip()
                if not code:
                    continue
                concept = clean_concept_id(code)
                var_meta[concept] = {
                    "name": str(row.get("variable_name", "")).strip(),
                    "description": str(row.get("description", "")).strip(),
                }
            print(
                f"Loaded metadata for {len(var_meta)} concepts from {VARIABLE_DEFINITIONS_FILE}"
            )
        except Exception as e:
            print(f"Warning: failed reading {VARIABLE_DEFINITIONS_FILE}: {e}")
    else:
        print(
            f"No {VARIABLE_DEFINITIONS_FILE} found. Using source columns and terms headings."
        )

    concept_rows = []
    for concept in source_concepts:
        meta = var_meta.get(concept, {})
        name = meta.get("name") or humanize_concept_name(concept)
        description = meta.get("description") or ""

        # Fallback description from terms section title by concept prefix (e.g. 2a, 6b)
        if not description:
            prefix = concept.split("_", 1)[0].lower()
            if prefix in section_titles:
                description = f"FRA 2025 section {prefix}: {section_titles[prefix]}"

        concept_rows.append(
            {
                "concept": concept,
                "name": name,
                "description": description,
                "concept_type": concept_types.get(concept, "measure"),
                "domain": "",
            }
        )

    concepts_df = pd.DataFrame(concept_rows)

    # Add standard DDF concepts
    standard_concepts = pd.DataFrame(
        [
            {
                "concept": "geo",
                "name": "Geographic Entity",
                "description": "Geographic entity identifier",
                "concept_type": "entity_domain",
                "domain": "",
            },
            {
                "concept": "year",
                "name": "Year",
                "description": "Year",
                "concept_type": "time",
                "domain": "",
            },
            {
                "concept": "name",
                "name": "Name",
                "description": "Name of the entity",
                "concept_type": "string",
                "domain": "",
            },
            {
                "concept": "domain",
                "name": "Domain",
                "description": "Entity set domain for the concept",
                "concept_type": "string",
                "domain": "",
            },
            {
                "concept": "description",
                "name": "Description",
                "description": "Description",
                "concept_type": "string",
                "domain": "",
            },
        ]
    )

    all_concepts = pd.concat([standard_concepts, concepts_df], ignore_index=True)
    all_concepts = (
        all_concepts.drop_duplicates(subset=["concept"])
        .sort_values("concept")
        .reset_index(drop=True)
    )

    # Save concepts file
    all_concepts.to_csv(CONCEPTS_DIR, index=False)
    print(f"Created concepts file: {CONCEPTS_DIR}")
    print(f"Total concepts: {len(all_concepts)}")


def coerce_series_for_output(series):
    """
    Prefer numeric datapoints when possible; keep string values (e.g. yes/no) otherwise.
    """
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().any():
        return numeric

    # Keep non-empty strings if no numeric values are present.
    s = series.astype(str).str.strip()
    s = s.replace({"": pd.NA, "nan": pd.NA, "NaN": pd.NA, "N/A": pd.NA})
    return s


def process_file(filepath, output_dir):
    """
    Reads a source CSV file and creates a DDF datapoint CSV for each variable column.
    """
    if not os.path.exists(filepath):
        print(f"Source file not found: {filepath}. Skipping.")
        return

    print(f"Processing {filepath}...")

    try:
        df = pd.read_csv(
            filepath, na_values=["", " ", "NaN", "N/A"], encoding="utf-8-sig"
        )
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return

    # Drop legacy climatic-domain split columns that are not datapoints.
    existing_columns_to_drop = [col for col in CLIMATE_DOMAIN_COLS if col in df.columns]
    if existing_columns_to_drop:
        df = df.drop(columns=existing_columns_to_drop)
        print(f"  Dropped columns: {existing_columns_to_drop}")

    # Check if the essential id_vars exist in the dataframe
    if not all(col in df.columns for col in ID_VARS):
        print(f"Skipping {filepath}: does not contain required columns {ID_VARS}.")
        return

    # Identify variable columns (i.e., not primary keys or descriptive columns)
    variable_cols = [col for col in df.columns if col not in ID_VARS + DESCRIPTIVE_VARS]

    for var_original in variable_cols:
        # Create a clean concept ID
        var_clean = clean_concept_id(var_original)

        print(f"  Generating datapoint for variable: {var_original} -> {var_clean}")

        # Create a new DataFrame for the specific datapoint
        datapoint_df = df[ID_VARS + [var_original]].copy()

        # Rename 'iso3' to 'geo' as requested
        datapoint_df.rename(columns={"iso3": "geo"}, inplace=True)

        # Drop rows where the variable's value is missing
        datapoint_df.dropna(subset=[var_original], inplace=True)

        # Skip if no data is left after dropping missing values
        if datapoint_df.empty:
            print(f"  Skipping empty datapoint file for {var_original}")
            continue

        # Rename the variable column to its clean name
        datapoint_df.rename(columns={var_original: var_clean}, inplace=True)

        # Clean and set data types
        datapoint_df["year"] = pd.to_numeric(
            datapoint_df["year"], errors="coerce"
        ).astype("Int64")
        datapoint_df["geo"] = datapoint_df["geo"].astype(str).str.lower()

        datapoint_df[var_clean] = coerce_series_for_output(datapoint_df[var_clean])
        datapoint_df.dropna(subset=["year", "geo", var_clean], inplace=True)

        # Final check if dataframe has content before saving
        if datapoint_df.empty:
            print(
                f"  Skipping empty datapoint file for {var_original} after data cleaning."
            )
            continue

        # Define the output path and filename according to DDF conventions
        output_filename = f"ddf--datapoints--{var_clean}--by--geo--year.csv"
        output_path = os.path.join(output_dir, output_filename)

        # Save the processed data to a new CSV file
        datapoint_df.to_csv(output_path, index=False)


def clean_existing_datapoints(output_dir):
    """Remove existing generated datapoints to avoid stale concepts between FRA releases."""
    pattern = os.path.join(output_dir, "ddf--datapoints--*--by--geo--year.csv")
    existing = glob.glob(pattern)
    for path in existing:
        os.remove(path)
    print(f"Removed {len(existing)} existing datapoint files from {output_dir}")


def main():
    """Main function to run the ETL process."""
    print("Starting ETL process...")

    # Ensure the output directory exists
    if not os.path.exists(DATAPOINTS_DIR):
        os.makedirs(DATAPOINTS_DIR)
        print(f"Created output directory: {DATAPOINTS_DIR}")

    source_files = get_source_files()

    clean_existing_datapoints(DATAPOINTS_DIR)

    # Create geo entities file
    create_geo_entities(source_files)

    # Create concepts file
    create_concepts_file(source_files)

    # Process both specified files for datapoints
    for source_file in source_files:
        process_file(source_file, DATAPOINTS_DIR)

    print("ETL process completed.")


if __name__ == "__main__":
    main()
