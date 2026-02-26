# Global Forest Resources Assessments (FRA 2025)

## Source
- Assessment data: https://fra-data.fao.org/assessments/fra/2025/WO/home/overview
- Definitions page: https://fra-data.fao.org/definitions/fra/2025/en/

Place these files in `etl/source/`:
- `FRA_Years_<date>.csv`
- `Annual_<date>.csv`
- `Terms and Definitions.md`

`Terms and Definitions.md` is downloaded from the FRA 2025 definitions page using a web-to-markdown browser extension.

## Update Workflow
From repo root:

```bash
# optional: create environment
uv venv .venv
uv pip install pandas

# regenerate variable definitions from current source files
.venv/bin/python etl/scripts/generate_variable_definitions.py

# rebuild dataset outputs
.venv/bin/python etl/scripts/_etl.py

# validate and refresh datapackage
validate-ddf-ng -p
```

If `validate-ddf-ng -p` shows datapackage resource-list errors on first run, run it again.  
The first run may regenerate `datapackage.json`; the second run should pass.
