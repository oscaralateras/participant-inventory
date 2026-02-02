from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.core.file_ingest import load_dataset_frame
from src.core.schema_registry import SchemaRegistry, load_schema_registry

logger = logging.getLogger(__name__)


def bulk_load_datasets(
    *,
    data_dir: str | Path,
    schema: SchemaRegistry,
    cache_dir: str | Path | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Bulk-load all datasets that have files present in a folder.

    For each dataset defined in datasets.yaml, this:
      - checks whether expected file exists in data_dir
      - if present, loads + standardises it via load_dataset_frame(...)
      - stores the cleaned DataFrame in a dict keyed by dataset name

    Optionally, writes cleaned outputs to Parquet in cache_dir.

    Args:
        data_dir: Folder containing uploaded/raw dataset files (CSV/XLSX).
        schema: Loaded SchemaRegistry.
        cache_dir: Optional folder to write cleaned Parquet files per dataset.

    Returns:
        Dict mapping dataset -> cleaned DataFrame.
    """
    # Convert data_dir to Path object for consistent path operations
    data_dir = Path(data_dir)

    # Validate input folder exists
    if not data_dir.exists() or not data_dir.is_dir():
        msg = f"data_dir must be an existing directory: '{data_dir.resolve()}'"
        logger.error(msg)
        raise ValueError(msg)

    # Set up optional output folder for cached parquet files
    cache_path: Path | None = None
    if cache_dir is not None:
        cache_path = Path(cache_dir)
        cache_path.mkdir(parents=True, exist_ok=True)  # Create if doesn't exist

    # Extract the datasets block from the loaded YAML schema
    datasets_block = schema.datasets_yaml.get("datasets", {}) or {}
    if not isinstance(datasets_block, dict):
        msg = "SchemaRegistry.datasets_yaml['datasets'] must be a dict"
        logger.error(msg)
        raise ValueError(msg)

    # Dictionary to store all successfully loaded datasets
    results: dict[str, pd.DataFrame] = {}

    # Iterate over all dataset names registered in the schema
    for dataset in sorted(schema.dataset_names):
        # Get configuration for this specific dataset from the YAML
        dataset_cfg = datasets_block.get(dataset, {}) or {}
        source_cfg = (dataset_cfg.get("source") or {})

        # Extract expected filename for this dataset
        file_name = source_cfg.get("file_name")
        if not file_name:
            logger.warning("Dataset '%s' missing source.file_name in datasets.yaml; skipping", dataset)
            continue

        # Build full path to the data file
        file_path = data_dir / str(file_name)

        # Skip datasets whose files aren't present in the data directory
        if not file_path.exists():
            logger.warning("Dataset '%s' file not found in data_dir; skipping: %s", dataset, file_path.name)
            continue

        # Load and standardize the dataset using file_ingest module
        # This handles: ID standardization, column renaming, required column validation
        try:
            df = load_dataset_frame(dataset=dataset, file_path=file_path, schema=schema)
        except Exception as e:
            logger.error("Failed to load dataset '%s' from '%s': %s", dataset, file_path.name, e)
            continue

        # Store successfully loaded dataset
        results[dataset] = df
        logger.info("Loaded dataset '%s' -> %d rows, %d cols", dataset, df.shape[0], df.shape[1])

        # Write cleaned dataset to parquet cache if cache_dir was provided
        if cache_path is not None:
            out_path = cache_path / f"{dataset}.parquet"
            df.to_parquet(out_path, index=False)
            logger.info("Wrote cache parquet for '%s' -> %s", dataset, out_path.name)

    logger.info("Bulk load complete: %d dataset(s) loaded", len(results))
    return results


if __name__ == "__main__":
    # Set up logging to see INFO level messages
    logging.basicConfig(level=logging.INFO)

    # Load the schema registry once (defines all datasets and variable mappings)
    schema = load_schema_registry("schema/datasets.yaml", "schema/variables.csv")

    # Load all datasets: raw files from data/raw, write cleaned cache to data/clean
    dfs = bulk_load_datasets(
        data_dir="data/raw",
        cache_dir="data/clean",
        schema=schema,
    )

    print(f"Loaded {len(dfs)} dataset(s): {list(dfs.keys())}")