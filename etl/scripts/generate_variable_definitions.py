import argparse
import csv
import glob
import os
import re
from typing import Dict, List, Optional, Tuple

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCE_DIR = os.path.join(BASE_DIR, "source")

DEFAULT_OUTPUT = os.path.join(SOURCE_DIR, "variable_definitions.csv")
TERMS_PATH = os.path.join(SOURCE_DIR, "Terms and Definitions.md")

ID_VARS = {"regions", "iso3", "iso2", "m49", "name", "Desk study", "deskStudy", "year"}
CLIMATE_DOMAIN_COLS = {"boreal", "subtropical", "temperate", "tropical"}


TOKEN_EXPANSIONS = {
    "tot": "total",
    "prim": "primary",
    "prot": "protection",
    "biodiv": "biodiversity",
    "socserv": "social services",
    "for": "forest",
    "mngt": "management",
    "inst": "institutions",
    "mr": "management rights",
    "fo": "forest ownership",
    "pfe": "permanent forest estate",
    "gs": "growing stock",
    "ha": "hectare",
    "bgb": "below ground biomass",
    "agb": "above ground biomass",
    "dwb": "dead wood biomass",
    "dw": "dead wood",
    "y_n": "yes no",
    "no": "no designation",
}

STOPWORDS = {
    "of",
    "and",
    "with",
    "for",
    "to",
    "the",
    "area",
    "forest",
    "land",
}


def clean_concept_id(concept_name: str) -> str:
    concept_name = concept_name.replace("#", "num")
    concept_name = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", concept_name)
    concept_name = re.sub(r"__([A-Z])", r"_\1", concept_name)
    concept_name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", concept_name)
    return concept_name.lower()


def get_latest_source_file(prefix: str) -> Optional[str]:
    pattern = os.path.join(SOURCE_DIR, f"{prefix}_*.csv")
    candidates = sorted(glob.glob(pattern))
    if not candidates:
        return None

    def sort_key(path: str) -> Tuple[int, str]:
        name = os.path.basename(path)
        m = re.search(r"(\d{4}[-_]\d{2}[-_]\d{2}|\d{4}_\d{2}_\d{2})", name)
        if m:
            return (1, m.group(1).replace("_", "-"))
        return (0, f"{os.path.getmtime(path):020.3f}")

    return sorted(candidates, key=sort_key)[-1]


def get_source_files() -> List[str]:
    files = [get_latest_source_file("FRA_Years"), get_latest_source_file("Annual")]
    files = [f for f in files if f]
    if not files:
        raise FileNotFoundError(
            f"No source files matching FRA_Years_*.csv or Annual_*.csv in {SOURCE_DIR}"
        )
    return files


def read_header(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        return next(reader)


def extract_variable_codes(source_files: List[str]) -> List[str]:
    seen = set()
    variable_codes = []
    for path in source_files:
        for col in read_header(path):
            if col in ID_VARS or col in CLIMATE_DOMAIN_COLS:
                continue
            if col not in seen:
                seen.add(col)
                variable_codes.append(col)
    return variable_codes


def parse_existing_variable_definitions(
    path: str,
) -> Tuple[Dict[str, dict], Dict[str, dict]]:
    by_code = {}
    by_concept = {}
    if not os.path.exists(path):
        return by_code, by_concept

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = (row.get("variable_code") or "").strip()
            if not code:
                continue
            item = {
                "variable_code": code,
                "variable_name": (row.get("variable_name") or "").strip(),
                "description": (row.get("description") or "").strip(),
            }
            by_code[code] = item
            by_concept[clean_concept_id(code)] = item
    return by_code, by_concept


def normalize_text(text: str) -> str:
    text = text.lower().strip()
    text = text.replace("_", " ")
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text


def tokenize(text: str) -> set:
    parts = normalize_text(text).split()
    return {p for p in parts if p and p not in STOPWORDS}


def parse_terms_and_definitions(
    path: str,
) -> Tuple[Dict[str, str], Dict[str, List[dict]]]:
    section_titles = {}
    section_terms = {}
    if not os.path.exists(path):
        return section_titles, section_terms

    current_section = None
    lines = open(path, "r", encoding="utf-8-sig").read().splitlines()
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        m_sec = re.match(r"^##\s+(\d+[a-zA-Z]?)\s+(.+)$", line)
        if m_sec:
            current_section = m_sec.group(1).lower()
            section_titles[current_section] = m_sec.group(2).strip()
            i += 1
            continue

        m_term = re.match(r"^###\s+(.+)$", line)
        if m_term and current_section:
            heading = m_term.group(1).strip().strip("_")
            j = i + 1
            definition_lines = []
            while j < len(lines):
                nxt = lines[j].strip()
                if nxt.startswith("### ") or nxt.startswith("## "):
                    break
                if nxt.startswith(">"):
                    definition_lines.append(nxt.lstrip(">").strip())
                elif definition_lines and nxt:
                    definition_lines.append(nxt)
                elif definition_lines and not nxt:
                    break
                j += 1

            definition = " ".join(definition_lines).strip()
            if definition:
                section_terms.setdefault(current_section, []).append(
                    {
                        "heading": heading,
                        "heading_tokens": tokenize(heading),
                        "definition": definition,
                    }
                )
            i = j
            continue

        i += 1

    return section_titles, section_terms


def prettify_variable_name(variable_code: str) -> str:
    # Keep ranking marker in display names.
    var = variable_code.replace("#", " #")
    var = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", var)
    var = var.replace("_", " ")
    var = re.sub(r"\s+", " ", var).strip()

    # Drop leading FRA section code from display name.
    var = re.sub(r"^\d+[a-zA-Z]\s+", "", var)
    return var.title().replace("Yn", "Y/N")


def expanded_tokens_for_variable(variable_code: str) -> set:
    work = variable_code
    for k, v in TOKEN_EXPANSIONS.items():
        work = work.replace(k, v)
    return tokenize(work)


def best_term_definition(
    variable_code: str, section_code: str, section_terms: Dict[str, List[dict]]
) -> Optional[str]:
    terms = section_terms.get(section_code, [])
    if not terms:
        return None

    var_tokens = expanded_tokens_for_variable(variable_code)
    best = (0, None)
    for t in terms:
        overlap = len(var_tokens.intersection(t["heading_tokens"]))
        if overlap > best[0]:
            best = (overlap, t["definition"])
    return best[1] if best[0] > 0 else None


def section_code_from_variable(variable_code: str) -> Optional[str]:
    m = re.match(r"^(\d+[a-zA-Z])_", variable_code)
    return m.group(1).lower() if m else None


def generate_variable_definitions(
    source_files: List[str], existing_path: str, terms_path: str
) -> List[dict]:
    variable_codes = extract_variable_codes(source_files)
    existing_by_code, existing_by_concept = parse_existing_variable_definitions(
        existing_path
    )
    section_titles, section_terms = parse_terms_and_definitions(terms_path)

    rows = []
    for code in variable_codes:
        concept = clean_concept_id(code)
        legacy = existing_by_code.get(code) or existing_by_concept.get(concept) or {}

        name = legacy.get("variable_name") or prettify_variable_name(code)
        desc = legacy.get("description", "")

        if not desc:
            sec = section_code_from_variable(code)
            desc = best_term_definition(code, sec, section_terms) if sec else None
            if not desc and sec and sec in section_titles:
                desc = f"FRA 2025 section {sec}: {section_titles[sec]}"

        rows.append(
            {
                "variable_code": code,
                "variable_name": name,
                "description": desc or "",
            }
        )

    return rows


def write_variable_definitions(rows: List[dict], output_path: str) -> None:
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["variable_code", "variable_name", "description"]
        )
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate variable_definitions.csv from FRA source files and Terms and Definitions."
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--existing",
        default=DEFAULT_OUTPUT,
        help=f"Existing variable definitions CSV for metadata reuse (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--terms",
        default=TERMS_PATH,
        help=f"Terms and Definitions markdown path (default: {TERMS_PATH})",
    )
    args = parser.parse_args()

    source_files = get_source_files()
    rows = generate_variable_definitions(source_files, args.existing, args.terms)
    write_variable_definitions(rows, args.output)

    print("Generated variable definitions:")
    print(f"  output: {args.output}")
    print(f"  variables: {len(rows)}")
    print("  source files:")
    for f in source_files:
        print(f"    - {os.path.basename(f)}")


if __name__ == "__main__":
    main()
