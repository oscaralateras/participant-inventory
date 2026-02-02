from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SchemaRegistry:
    """
    In-memory representation of the schema contract.

    This bundles:
      - raw YAML config
      - raw variable rows from CSV
      - precomputed lookup maps used by ingestion/validation
    """

    datasets_yaml: dict[str, Any]
    variables_rows: list[dict[str, str]]

    # Convenience sets
    dataset_names: set[str]
    variables_dataset_names: set[str]

    # Lookup maps
    variables_by_dataset: dict[str, list[dict[str, str]]]
    source_to_canonical_by_dataset: dict[str, dict[str, str]]
    required_vars_by_dataset: dict[str, set[str]]
    sql_types_by_dataset: dict[str, dict[str, str]]  


    # ID rules
    participant_id_column: str
    id_aliases: dict[str, list[str]]


# -----------------------------
# Helper functions
# -----------------------------
def _ensure_file_exists(path: Path, label: str) -> None:
    """
    Ensure a schema file exists and is a file (not a directory).

    Args:
        path: Path to the schema file.
        label: Human-friendly name used in error messages (e.g., "datasets.yaml").

    Raises:
        FileNotFoundError: If the path does not exist or is not a file.
    """
    # Check the path exists on disk
    if not path.exists():
        msg = f"Missing required schema file: {label} at '{path.resolve()}'"
        logger.error(msg)
        raise FileNotFoundError(msg)

    # Check it's a file (not a folder)
    if not path.is_file():
        msg = f"Schema path is not a file: {label} at '{path.resolve()}'"
        logger.error(msg)
        raise FileNotFoundError(msg)


def _load_yaml(path: Path) -> dict[str, Any]:
    """
    Load a YAML file and return its contents as a dictionary.

    Args:
        path: Path to the YAML file.

    Returns:
        Parsed YAML content as a dict. If the YAML is empty, returns {}.

    Raises:
        ValueError: If the YAML cannot be parsed or the top-level is not a dict.
    """
    try:
        # Parse YAML into Python objects
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except Exception as e:
        msg = f"Failed to parse YAML '{path}': {e}"
        logger.error(msg)
        raise ValueError(msg) from e

    # Enforce expected top-level structure for datasets.yaml
    if not isinstance(data, dict):
        msg = f"datasets.yaml must parse to a dict at top-level, got {type(data)}"
        logger.error(msg)
        raise ValueError(msg)

    return data


def _load_csv(path: Path) -> list[dict[str, str]]:
    """
    Load variables.csv and return its rows as a list of dictionaries.

    Args:
        path: Path to the CSV file.

    Returns:
        List of rows, where each row is a dict[str, str].

    Raises:
        ValueError: If required headers are missing or if any row is invalid.
    """
    required_headers = {"dataset", "source_column", "variable_name", "is_required", "sql_type"}  # Add sql_type

    try:
        # Open and parse the CSV into dict rows keyed by the header names
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)

            # Validate headers exist
            headers = set(reader.fieldnames or [])
            missing = sorted(required_headers - headers)
            if missing:
                msg = f"variables.csv missing required header(s): {missing}"
                logger.error(msg)
                raise ValueError(msg)

            rows: list[dict[str, str]] = []
            for line_no, row in enumerate(reader, start=2):  # header is line 1
                # Strip whitespace and normalise empty values
                cleaned = {k: (v or "").strip() for k, v in row.items()}

                # Fail fast if key fields are missing/blank
                if not cleaned["dataset"] or not cleaned["source_column"] or not cleaned["variable_name"]:
                    msg = (
                        f"Invalid variables.csv row at line {line_no}: "
                        "dataset/source_column/variable_name must be non-empty"
                    )
                    logger.error(msg)
                    raise ValueError(msg)

                rows.append(cleaned)

    except ValueError:
        # Re-raise our own validation errors unchanged
        raise
    except Exception as e:
        msg = f"Failed to read CSV '{path}': {e}"
        logger.error(msg)
        raise ValueError(msg) from e

    return rows


def _extract_dataset_names(datasets_yaml: dict[str, Any]) -> set[str]:
    """
    Extract dataset identifiers from the parsed datasets.yaml content.

    Args:
        datasets_yaml: Parsed YAML content as a dict.

    Returns:
        A set of dataset names (keys under the top-level "datasets" mapping).

    Raises:
        ValueError: If "datasets" is missing or is not a dict.
    """
    # Pull out the top-level "datasets" mapping
    datasets = datasets_yaml.get("datasets", {})

    # Validate expected structure
    if not isinstance(datasets, dict):
        msg = "datasets.yaml must contain a top-level 'datasets' mapping"
        logger.error(msg)
        raise ValueError(msg)

    # Dataset names are the keys of the mapping
    return set(datasets.keys())


def _group_by_dataset(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    """
    Group variable rows by their dataset name.

    Args:
        rows: List of variable rows from variables.csv.

    Returns:
        Dict mapping dataset name -> list of rows for that dataset.
    """
    grouped: dict[str, list[dict[str, str]]] = {}

    # Each row has a "dataset" field; group rows under that key
    for r in rows:
        dataset = r["dataset"]
        grouped.setdefault(dataset, []).append(r)

    return grouped


def _build_lookup_maps(
    variables_by_dataset: dict[str, list[dict[str, str]]],
) -> tuple[dict[str, dict[str, str]], dict[str, set[str]], dict[str, dict[str, str]]]:  # Add third return type
    """
    Build lookup maps used by ingestion/validation.

    Args:
        variables_by_dataset: Dict mapping dataset -> list of variable rows.

    Returns:
        (source_to_canonical_by_dataset, required_vars_by_dataset, sql_types_by_dataset)

        source_to_canonical_by_dataset:
            dataset -> {source_column -> variable_name}

        required_vars_by_dataset:
            dataset -> set(variable_name) for rows marked required
            
        sql_types_by_dataset:
            dataset -> {variable_name -> sql_type}
    """
    source_to_canonical: dict[str, dict[str, str]] = {}
    required_vars: dict[str, set[str]] = {}
    sql_types: dict[str, dict[str, str]] = {}  

    truthy = {"true", "1", "yes", "y"}

    for dataset, rows in variables_by_dataset.items():
        source_map: dict[str, str] = {}
        required_set: set[str] = set()
        types_map: dict[str, str] = {}  

        for r in rows:
            src = r["source_column"]
            canon = r["variable_name"]
            sql_type = r.get("sql_type", "TEXT").strip().upper()  

            # Prevent ambiguous mappings like: Age -> age AND Age -> age_years
            if src in source_map and source_map[src] != canon:
                msg = (
                    f"Ambiguous mapping in variables.csv for dataset '{dataset}': "
                    f"source_column '{src}' maps to both '{source_map[src]}' and '{canon}'"
                )
                logger.error(msg)
                raise ValueError(msg)

            source_map[src] = canon
            types_map[canon] = sql_type  

            # Mark required variables
            if (r.get("is_required") or "").strip().lower() in truthy:
                required_set.add(canon)

        source_to_canonical[dataset] = source_map
        required_vars[dataset] = required_set
        sql_types[dataset] = types_map  

    return source_to_canonical, required_vars, sql_types  # Update return

# -----------------------------
# Main loader
# -----------------------------
def load_schema_registry(
    datasets_yaml_path: str | Path,
    variables_csv_path: str | Path,
) -> SchemaRegistry:
    """
    Load and validate the schema contract from datasets.yaml + variables.csv.

    Validation:
      1) Both files exist
      2) variables.csv contains required headers
      3) every dataset referenced in variables.csv exists in datasets.yaml
      4) no ambiguous duplicate mappings within a dataset (same source_column -> different variable_name)

    Returns:
      SchemaRegistry with raw config + helpful lookup maps.
    """
    # Convert input paths into Path objects so we can use consistent path operations
    datasets_yaml_path = Path(datasets_yaml_path)
    variables_csv_path = Path(variables_csv_path)

    # Fail fast if either schema file is missing
    _ensure_file_exists(datasets_yaml_path, label="datasets.yaml")
    _ensure_file_exists(variables_csv_path, label="variables.csv")

    # Load raw schema files into Python structures
    datasets_yaml = _load_yaml(datasets_yaml_path)
    variables_rows = _load_csv(variables_csv_path)

    # Extract the dataset names defined in the datasets.yaml and variables.csv
    dataset_names = _extract_dataset_names(datasets_yaml)
    variables_dataset_names = {row["dataset"] for row in variables_rows}

    # Check that variables.csv doesn't reference datasets not present in datasets.yaml
    missing_in_yaml = sorted(variables_dataset_names - dataset_names)
    if missing_in_yaml:
        msg = (
            "Schema mismatch: variables.csv references dataset(s) not present in datasets.yaml: "
            f"{missing_in_yaml}"
        )
        logger.error(msg)
        raise ValueError(msg)

    # Group variables by dataset for easy access during ingestion
    variables_by_dataset = _group_by_dataset(variables_rows)

    # Build maps used to rename columns and validate required fields
    source_to_canonical_by_dataset, required_vars_by_dataset, sql_types_by_dataset = _build_lookup_maps(variables_by_dataset)  # Add sql_types_by_dataset

    # Read canonical participant ID column name (default to "participant_id" if missing)
    participant_id_column = str(datasets_yaml.get("participant_id_column", "participant_id"))

    # Read ID alias mapping (e.g., participant_id -> [SubjID])
    id_aliases = datasets_yaml.get("id_column_aliases", {}) or {}
    if not isinstance(id_aliases, dict):
        msg = "datasets.yaml field 'id_column_aliases' must be a mapping/dict"
        logger.error(msg)
        raise ValueError(msg)

    # Log a summary so you can see what loaded successfully
    logger.info(
        "Loaded schema registry: %d datasets, %d variables rows",
        len(dataset_names),
        len(variables_rows),
    )

    # Log per-dataset summary (useful for sanity checks)
    for ds in sorted(dataset_names):
        logger.info(
            "Dataset '%s': %d variables (%d required)",
            ds,
            len(variables_by_dataset.get(ds, [])),
            len(required_vars_by_dataset.get(ds, set())),
        )

    # Return a single structured object that the rest of the pipeline can use
    return SchemaRegistry(
        datasets_yaml=datasets_yaml,
        variables_rows=variables_rows,
        dataset_names=dataset_names,
        variables_dataset_names=variables_dataset_names,
        variables_by_dataset=variables_by_dataset,
        source_to_canonical_by_dataset=source_to_canonical_by_dataset,
        required_vars_by_dataset=required_vars_by_dataset,
        sql_types_by_dataset=sql_types_by_dataset,  
        participant_id_column=participant_id_column,
        id_aliases=id_aliases,
    )

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    schema = load_schema_registry("schema/datasets.yaml", "schema/variables.csv")
    print(f"Datasets: {len(schema.dataset_names)}")
    print(f"Variables: {len(schema.variables_rows)}")