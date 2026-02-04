import logging
from sqlalchemy import text
from src.core.schema_registry import SchemaRegistry
from src.db.connection import get_engine

logger = logging.getLogger(__name__)

def create_inventory_summary_view(schema: SchemaRegistry, engine) -> None:
    """
    Create a materialized view with participant demographics and dataset availability flags.
    
    Includes:
    - All columns from basic_covariates (demographics)
    - Total scores from individual_symptoms (not individual items)
    - Boolean flags: has_dti, has_cortical_thickness, has_subcortical_volumes, has_cortical_surface_area
    
    Args:
        schema: SchemaRegistry with dataset definitions
        engine: SQLAlchemy engine
    """
    # Get columns from individual_symptoms that aren't already in basic_covariates
    bc_columns = set(schema.sql_types_by_dataset['basic_covariates'].keys())
    ind_columns = [col for col in schema.sql_types_by_dataset['individual_symptoms'].keys() 
                   if col != schema.participant_id_column 
                   and '_total' in col
                   and col not in bc_columns]  
    
    # Build the SELECT clause for individual_symptoms columns
    # Result: "ind.bdi_total, ind.hdrs_total, ind.ids_total, ..."
    ind_select = ", ".join([f"ind.{col}" for col in ind_columns])
    
    # Build the complete CREATE MATERIALIZED VIEW SQL statement
    # Materialized view stores the result physically (unlike regular views which re-compute)
    create_view_sql = f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS inventory_summary AS
    SELECT 
        bc.*,
        {ind_select},
        CASE WHEN dti.participant_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_dti,
        CASE WHEN ct.participant_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_cortical_thickness,
        CASE WHEN sv.participant_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_subcortical_volumes,
        CASE WHEN csa.participant_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_cortical_surface_area
    FROM basic_covariates bc
    LEFT JOIN individual_symptoms ind ON bc.participant_id = ind.participant_id
    LEFT JOIN dti ON bc.participant_id = dti.participant_id
    LEFT JOIN cortical_thickness ct ON bc.participant_id = ct.participant_id
    LEFT JOIN subcortical_volumes sv ON bc.participant_id = sv.participant_id
    LEFT JOIN cortical_surface_area csa ON bc.participant_id = csa.participant_id
    """
    # Execute the SQL to create the materialized view
    with engine.connect() as conn:
        conn.execute(text(create_view_sql))
        conn.commit()

    logger.info("Created materialized view 'inventory_summary'")

def create_full_data_view(schema: SchemaRegistry, engine) -> None:
    """Create full data view with all columns from all datasets."""
    
    # Start with all basic_covariates columns
    included_columns = set(schema.sql_types_by_dataset['basic_covariates'].keys())
    
    # Build select clauses for each dataset
    datasets = [
        ('ind', 'individual_symptoms'),
        ('dti', 'dti'),
        ('ct', 'cortical_thickness'),
        ('sv', 'subcortical_volumes'),
        ('csa', 'cortical_surface_area')
    ]
    
    select_parts = []
    for alias, dataset_name in datasets:
        # Get columns that haven't been included yet
        cols = [c for c in schema.sql_types_by_dataset[dataset_name].keys() 
                if c not in included_columns]
        # Track these columns so we skip them in future datasets
        included_columns.update(cols)
        # Build "alias.col1, alias.col2, ..." string
        if cols:  # Only add if there are columns
            select_parts.append(", ".join([f"{alias}.{c}" for c in cols]))
    
    # Join all parts
    all_selects = ",\n        ".join(select_parts)
    
    create_view_sql = f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS full_data_view AS
    SELECT 
        bc.*,
        {all_selects}
    FROM basic_covariates bc
    LEFT JOIN individual_symptoms ind ON bc.participant_id = ind.participant_id
    LEFT JOIN dti ON bc.participant_id = dti.participant_id
    LEFT JOIN cortical_thickness ct ON bc.participant_id = ct.participant_id
    LEFT JOIN subcortical_volumes sv ON bc.participant_id = sv.participant_id
    LEFT JOIN cortical_surface_area csa ON bc.participant_id = csa.participant_id
    """
    
    # Execute the SQL to create the materialized view in Postgres
    with engine.connect() as conn:
        conn.execute(text(create_view_sql))
        conn.commit()
    
    logger.info("Created materialized view 'full_data_view'")
    
if __name__ == "__main__":
    import logging
    from src.core.schema_registry import load_schema_registry
    
    logging.basicConfig(level=logging.INFO)
    
    schema = load_schema_registry("schema/datasets.yaml", "schema/variables.csv")
    engine = get_engine()
    
    # Create both views
    create_inventory_summary_view(schema, engine)
    create_full_data_view(schema, engine)
    
    print("\n✓ Both materialized views created!")