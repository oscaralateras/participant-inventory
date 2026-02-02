from __future__ import annotations

# Bulk import orchestration:
# - scans a folder for raw dataset files
# - uses load_dataset_frame(...) to standardise each dataset
# - returns a dict of cleaned DataFrames (dataset -> df)
# - optionally writes cleaned Parquet files for caching/reuse

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
      - checks whether the expected file exists in data_dir
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
    data_dir = Path(data_dir)

    # Validate input folder exists
    if not data_dir.exists() or not data_dir.is_dir():
        msg = f"data_dir must be an existing directory: '{data_dir.resolve()}'"
        logger.error(msg)
        raise ValueError(msg)

    # Optional output folder for cached parquet files
    cache_path: Path | None = None
    if cache_dir is not None:
        cache_path = Path(cache_dir)
        cache_path.mkdir(parents=True, exist_ok=True)

    # Pull dataset definitions from the schema YAML
    datasets_block = schema.datasets_yaml.get("datasets", {}) or {}
    if not isinstance(datasets_block, dict):
        msg = "SchemaRegistry.datasets_yaml['datasets'] must be a dict"
        logger.error(msg)
        raise ValueError(msg)

    results: dict[str, pd.DataFrame] = {}

    # Iterate over datasets registered in the schema
    for dataset in sorted(schema.dataset_names):
        dataset_cfg = datasets_block.get(dataset, {}) or {}
        source_cfg = (dataset_cfg.get("source") or {})

        # Expected filename for this dataset
        file_name = source_cfg.get("file_name")
        if not file_name:
            logger.warning("Dataset '%s' missing source.file_name in datasets.yaml; skipping", dataset)
            continue

        file_path = data_dir / str(file_name)

        # Skip datasets that are not present in this folder
        if not file_path.exists():
            logger.warning("Dataset '%s' file not found in data_dir; skipping: %s", dataset, file_path.name)
            continue

        # Load + standardise via file_ingest
        try:
            df = load_dataset_frame(dataset=dataset, file_path=file_path, schema=schema)
        except Exception as e:
            logger.error("Failed to load dataset '%s' from '%s': %s", dataset, file_path.name, e)
            continue

        results[dataset] = df
        logger.info("Loaded dataset '%s' -> %d rows, %d cols", dataset, df.shape[0], df.shape[1])

        # Optionally cache as parquet
        if cache_path is not None:
            out_path = cache_path / f"{dataset}.parquet"
            df.to_parquet(out_path, index=False)
            logger.info("Wrote cache parquet for '%s' -> %s", dataset, out_path.name)

    logger.info("Bulk load complete: %d dataset(s) loaded", len(results))
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Load schema once
    schema = load_schema_registry("schema/datasets.yaml", "schema/variables.csv")

    # Convention: raw uploads go in data/raw, cleaned cache goes in data/clean
    dfs = bulk_load_datasets(
        data_dir="data/raw",
        cache_dir="data/clean",
        schema=schema,
    )

    print(f"Loaded {len(dfs)} dataset(s): {list(dfs.keys())}")