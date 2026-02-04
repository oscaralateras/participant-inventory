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
    
    # Execute the SQL to create the materialized view in Postgres
    with engine.connect() as conn:
        conn.execute(text(create_view_sql))
        conn.commit()
    
    logger.info("Created materialized view 'inventory_summary'")

if __name__ == "__main__":
    import logging
    from src.core.schema_registry import load_schema_registry
    
    # Set up logging to see INFO messages
    logging.basicConfig(level=logging.INFO)
    
    # Load schema registry
    schema = load_schema_registry("schema/datasets.yaml", "schema/variables.csv")
    
    # Get database engine
    engine = get_engine()
    
    # Create the inventory summary view
    create_inventory_summary_view(schema, engine)
    
    print("\n✓ Inventory summary view created!")