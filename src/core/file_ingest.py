from __future__ import annotations

# This module standardises uploaded dataset files (CSV/XLSX) into canonical DataFrames
# using the SchemaRegistry contract (column mappings, ID rules, required fields).

import logging
from pathlib import Path

import pandas as pd

from src.core.schema_registry import SchemaRegistry

logger = logging.getLogger(__name__)


# -----------------------------
# Helper functions
# -----------------------------

def _ensure_dataset_known(schema: SchemaRegistry, dataset: str) -> None:
    """Fail fast if the dataset key is not defined in the SchemaRegistry."""
    if dataset not in schema.dataset_names:
        msg = f"Unknown dataset '{dataset}'. Known datasets: {sorted(schema.dataset_names)}"
        logger.error(msg)
        raise ValueError(msg)


def _ensure_file_exists(path: Path) -> None:
    """Fail fast if file is missing, not a file, or empty."""
    if not path.exists():
        msg = f"Input file does not exist: '{path.resolve()}'"
        logger.error(msg)
        raise FileNotFoundError(msg)

    if not path.is_file():
        msg = f"Input path is not a file: '{path.resolve()}'"
        logger.error(msg)
        raise FileNotFoundError(msg)

    if path.stat().st_size == 0:
        msg = f"Input file is empty (0 bytes): '{path.resolve()}'"
        logger.error(msg)
        raise ValueError(msg)

def _get_dataset_source_config(schema: SchemaRegistry, dataset: str) -> tuple[str, str | None, int | None]:
    """
    Read how this dataset is stored in datasets.yaml:
      - kind: csv/xlsx
      - sheet_name: required for xlsx
      - header_row: optional for xlsx (0-based; default 0)
    """
    datasets_block = schema.datasets_yaml.get("datasets", {})
    dataset_cfg = datasets_block.get(dataset, {})
    source_cfg = (dataset_cfg.get("source") or {})

    # Determine file kind (csv/xlsx)
    kind = (source_cfg.get("kind") or "").strip().lower()
    if not kind:
        msg = f"datasets.yaml missing source.kind for dataset '{dataset}'"
        logger.error(msg)
        raise ValueError(msg)

    if kind not in {"csv", "xlsx"}:
        msg = f"Unsupported source.kind '{kind}' for dataset '{dataset}' (expected csv or xlsx)"
        logger.error(msg)
        raise ValueError(msg)

    sheet_name: str | None = None
    header_row: int | None = None

    if kind == "xlsx":
        # Sheet name is required for xlsx
        sheet_name_val = source_cfg.get("sheet_name")
        if sheet_name_val is None or str(sheet_name_val).strip() == "":
            msg = f"datasets.yaml missing source.sheet_name for xlsx dataset '{dataset}'"
            logger.error(msg)
            raise ValueError(msg)
        sheet_name = str(sheet_name_val)

        # Header row is optional; default to row 0 (first row)
        header_val = source_cfg.get("header_row", 0)
        try:
            header_row = int(header_val)
        except Exception as e:
            msg = f"datasets.yaml source.header_row must be an integer for dataset '{dataset}', got: {header_val!r}"
            logger.error(msg)
            raise ValueError(msg) from e

        if header_row < 0:
            msg = f"datasets.yaml source.header_row must be >= 0 for dataset '{dataset}', got: {header_row}"
            logger.error(msg)
            raise ValueError(msg)

        logger.info("Dataset '%s' source: kind=%s sheet=%s header_row=%d", dataset, kind, sheet_name, header_row)
    else:
        logger.info("Dataset '%s' source: kind=%s", dataset, kind)

    return kind, sheet_name, header_row


def _read_to_df(path: Path, kind: str, sheet_name: str | None, header_row: int | None) -> pd.DataFrame:
    """
    Read a dataset file into a pandas DataFrame (all values as strings).

    Args:
        path: Path to the file.
        kind: "csv" or "xlsx".
        sheet_name: Required for xlsx, ignored for csv.
        header_row: Header row index (0-based) for xlsx, ignored for csv.
    """
    try:
        if kind == "csv":
            df = pd.read_csv(path, dtype=str)

        elif kind == "xlsx":
            if sheet_name is None:
                msg = f"sheet_name is required for xlsx files: '{path.resolve()}'"
                logger.error(msg)
                raise ValueError(msg)

            # Default to first row if not provided
            effective_header = 0 if header_row is None else header_row

            df = pd.read_excel(
                path,
                sheet_name=sheet_name,
                header=effective_header,
                dtype=str,
            )

        else:
            msg = f"Unsupported file kind '{kind}' for '{path.resolve()}'"
            logger.error(msg)
            raise ValueError(msg)

    except ValueError:
        raise
    except Exception as e:
        msg = f"Failed to read {kind} file '{path.resolve()}': {e}"
        logger.error(msg)
        raise ValueError(msg) from e

    if df.empty:
        msg = f"No rows found in file '{path.resolve()}'"
        logger.error(msg)
        raise ValueError(msg)

    logger.info(
        "Read file '%s' (%s): %d rows, %d columns",
        path.name,
        kind,
        df.shape[0],
        df.shape[1],
    )
    return df


def _standardize_participant_id(df: pd.DataFrame, schema: SchemaRegistry) -> pd.DataFrame:
    """
    Ensure the DataFrame contains the canonical participant ID column name.

    - Prefer canonical ID if present.
    - Else rename first matching alias to canonical.
    - Drop rows where participant_id is blank.
    """
    canonical = schema.participant_id_column
    aliases = schema.id_aliases.get(canonical, []) or []

    # Prefer canonical column if present
    if canonical in df.columns:
        id_col = canonical
    else:
        # Otherwise try aliases
        id_col = None
        for a in aliases:
            if a in df.columns:
                id_col = a
                break

        if id_col is None:
            msg = (
                f"Missing participant ID column. Expected '{canonical}' or one of aliases {aliases}. "
                f"Found columns: {list(df.columns)}"
            )
            logger.error(msg)
            raise ValueError(msg)

        df = df.rename(columns={id_col: canonical})

    # Normalise + drop blank IDs
    before = len(df)
    df[canonical] = df[canonical].fillna("").astype(str).str.strip()
    df = df[df[canonical] != ""].copy()
    dropped = before - len(df)

    if dropped > 0:
        logger.info("Dropped %d row(s) with blank '%s'", dropped, canonical)

    return df


def _rename_and_filter_to_canonical(
    df: pd.DataFrame,
    mapping: dict[str, str],
    *,
    dataset: str,
    participant_id_column: str,
) -> pd.DataFrame:
    """
    Drop unknown columns, rename known columns to canonical names, and error on duplicates.

    Note: participant_id_column is always preserved as a system invariant, even if it is not
    part of the dataset-specific mapping.
    """
    original_cols = list(df.columns)

    # Keep mapped columns + always keep participant_id
    keep_cols = [c for c in df.columns if (c in mapping) or (c == participant_id_column)]
    dropped_cols = [c for c in df.columns if c not in keep_cols]
    if dropped_cols:
        logger.info(
            "Dataset '%s': dropping %d unknown column(s): %s",
            dataset,
            len(dropped_cols),
            dropped_cols,
        )

    df = df[keep_cols].copy()

    # Rename mapped columns to canonical (participant_id stays as-is)
    rename_map = {c: mapping[c] for c in keep_cols if c in mapping}
    df = df.rename(columns=rename_map)

    # Fail if duplicates appear after renaming
    if df.columns.duplicated().any():
        dups = df.columns[df.columns.duplicated()].tolist()
        msg = (
            f"Dataset '{dataset}': duplicate canonical columns after renaming: {dups}. "
            f"Original columns: {original_cols}"
        )
        logger.error(msg)
        raise ValueError(msg)

    renamed_count = sum(1 for c in keep_cols if (c in mapping and mapping[c] != c))
    logger.info(
        "Dataset '%s': kept %d/%d columns, renamed %d column(s)",
        dataset,
        len(keep_cols),
        len(original_cols),
        renamed_count,
    )

    return df

def _validate_required_columns(
    df: pd.DataFrame,
    *,
    dataset: str,
    required_vars: set[str],
    expected_vars: set[str],
) -> None:
    """
    Log completeness (present/expected) and fail if required vars are missing.
    """
    present_cols = set(df.columns)

    # Completeness summary
    present_expected = len(present_cols & expected_vars)
    total_expected = len(expected_vars)
    logger.info("Dataset '%s': columns present %d/%d (expected canonical columns)", dataset, present_expected, total_expected)

    # Required columns must exist
    missing_required = sorted(required_vars - present_cols)
    if missing_required:
        msg = f"Dataset '{dataset}': missing required column(s): {missing_required}"
        logger.error(msg)
        raise ValueError(msg)

    # Optional missing columns are informational only
    missing_optional = sorted((expected_vars - required_vars) - present_cols)
    if missing_optional:
        logger.info("Dataset '%s': missing %d optional column(s): %s", dataset, len(missing_optional), missing_optional)


# -----------------------------
# Public API
# -----------------------------

def load_dataset_frame(
    *,
    dataset: str,
    file_path: str | Path,
    schema: SchemaRegistry,
    sheet_name: str | None = None,
) -> pd.DataFrame:
    """
    Load one dataset file (CSV/XLSX), standardise it to canonical columns,
    validate required fields, and return a cleaned DataFrame.
    """
    file_path = Path(file_path)

    _ensure_dataset_known(schema, dataset)
    _ensure_file_exists(file_path)

    kind, schema_sheet_name, header_row = _get_dataset_source_config(schema, dataset)
    effective_sheet_name = sheet_name if sheet_name is not None else schema_sheet_name

    df = _read_to_df(file_path, kind, effective_sheet_name, header_row)

    # Ensure participant_id exists and is clean
    df = _standardize_participant_id(df, schema)

    # Rename and drop unknown columns for this dataset
    mapping = schema.source_to_canonical_by_dataset.get(dataset, {})
    if not mapping:
        msg = f"No column mapping found for dataset '{dataset}' in schema registry"
        logger.error(msg)
        raise ValueError(msg)

    df = _rename_and_filter_to_canonical(
    df,
    mapping,
    dataset=dataset,
    participant_id_column=schema.participant_id_column,
    )

    # Expected/required canonical variables
    expected_vars = set(mapping.values())
    required_vars = set(schema.required_vars_by_dataset.get(dataset, set()))

    # Always require participant_id as a safety invariant
    required_vars.add(schema.participant_id_column)

    _validate_required_columns(df, dataset=dataset, required_vars=required_vars, expected_vars=expected_vars)

    return df


