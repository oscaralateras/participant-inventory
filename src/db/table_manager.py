import logging
from sqlalchemy import text

from src.core.schema_registry import SchemaRegistry
from src.db.connection import get_engine

logger = logging.getLogger(__name__)


def create_table_for_dataset(
    dataset: str,
    schema: SchemaRegistry,
    engine,
) -> None:
    """
    Create a Postgres table for one dataset based on the schema.
    
    The table will have:
    - One column per variable in the dataset
    - SQL types from the schema registry
    - participant_id as PRIMARY KEY
    
    Args:
        dataset: Name of the dataset (becomes table name)
        schema: SchemaRegistry with variable definitions and SQL types
        engine: SQLAlchemy engine
    """
    # Step 1: Get the SQL type mapping for this specific dataset
    # Returns dict like: {'participant_id': 'TEXT', 'age': 'INTEGER', ...}
    type_map = schema.sql_types_by_dataset.get(dataset, {})

    # Fail fast if dataset doesn't exist in schema
    if not type_map:
        logger.error(f"No schema found for dataset '{dataset}'")
        raise ValueError(f"Unknown dataset: {dataset}")
    
    # Step 2: Build column definitions by looping through variables and their types
    column_defs = []

    for variable_name, sql_type in type_map.items():
        # participant_id gets special treatment - it's the PRIMARY KEY
        if variable_name == schema.participant_id_column:
            column_defs.append(f"{variable_name} {sql_type} PRIMARY KEY")
        else:
            # All other columns are just: column_name SQL_TYPE
            column_defs.append(f"{variable_name} {sql_type}")
    
    # Step 3: Join all column definitions with commas and newlines for readability
    columns_sql = ",\n    ".join(column_defs)

    # Step 4: Build the complete CREATE TABLE statement
    # IF NOT EXISTS means it won't error if table already exists
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {dataset} (
        {columns_sql}
    )
    """
    
    # Step 5: Execute the SQL to actually create the table
    with engine.connect() as conn:
        conn.execute(text(create_sql))
        conn.commit()  # Save the changes to the database
    
    logger.info(f"Created table '{dataset}' with {len(type_map)} columns")

def create_all_tables(schema: SchemaRegistry, engine) -> None:
    """
    Create tables for all datasets defined in the schema.
    
    Args:
        schema: SchemaRegistry with all dataset definitions
        engine: SQLAlchemy engine
    """
    # Loop through all datasets and create a table for each
    for dataset in sorted(schema.dataset_names):
        try:
            create_table_for_dataset(dataset, schema, engine)
        except Exception as e:
            logger.error(f"Failed to create table for '{dataset}': {e}")
            raise
    
    logger.info(f"Successfully created {len(schema.dataset_names)} tables")

if __name__ == "__main__":
    import logging
    from src.core.schema_registry import load_schema_registry
    
    logging.basicConfig(level=logging.INFO)
    
    schema = load_schema_registry("schema/datasets.yaml", "schema/variables.csv")
    engine = get_engine()
    
    # Create all tables
    create_all_tables(schema, engine)