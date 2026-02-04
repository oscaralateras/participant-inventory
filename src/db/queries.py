import logging
import pandas as pd
from sqlalchemy import text
from src.db.connection import get_engine

logger = logging.getLogger(__name__)

def count_with_filters(engine, **filters) -> int:
    """
    Count participants matching any combination of filters.
    
    Args:
        engine: SQLAlchemy engine
        **filters: Any column name with value to filter on
                   Examples: age_min=25, age_max=65, sex='0', dx='1', has_dti=True
    Returns:
        Count of matching participants
    """
    # Start with base SQL query
    sql_query = """
    SELECT COUNT(*) FROM inventory_summary
    """
    
    # Initialize empty list to collect WHERE conditions
    conditions = []

    # Loop through each filter passed by user
    for column_name, value in filters.items():
        
        # Handle minimum range filters (age_min, bdi_total_min, etc.)
        if column_name.endswith('_min'):
            # Extract actual column name: 'age_min' -> 'age'
            actual_column = column_name.replace('_min', '')
            # Build condition: "age >= 25"
            conditions.append(f"{actual_column} >= {value}")
        
        # Handle maximum range filters (age_max, bdi_total_max, etc.)
        elif column_name.endswith('_max'):
            # Extract actual column name: 'age_max' -> 'age'
            actual_column = column_name.replace('_max', '')
            # Build condition: "age <= 50"
            conditions.append(f"{actual_column} <= {value}")
        
        # Handle equality filters (sex, dx, has_dti, etc.)
        else:
            # Check if value is a string - strings need quotes in SQL
            if isinstance(value, str):
                # Build condition with quotes: "sex = '0'"
                conditions.append(f"{column_name} = '{value}'")
            else:
                # Build condition without quotes: "has_dti = True" or "age = 25"
                conditions.append(f"{column_name} = {value}")
    
    # Only add WHERE clause if there are conditions
    if len(conditions) > 0:
        # Join all conditions with AND: "age >= 25 AND sex = '0' AND has_dti = True"
        where_clause = " AND ".join(conditions)
        # Add WHERE clause to query
        sql_query += f" WHERE {where_clause}"
    
    # Execute the query and return count
    with engine.connect() as conn:
        result = conn.execute(text(sql_query))
        query_result = result.scalar()
        return query_result


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Get database engine
    engine = get_engine()
    
    # Test 1: Count all participants (no filters)
    total = count_with_filters(engine)
    print(f"Total participants: {total}")
    
    # Test 2: Count males only (sex='0' means male)
    males = count_with_filters(engine, sex='0')
    print(f"Male participants: {males}")
    
    # Test 3: Count participants in age range 30-50
    age_range = count_with_filters(engine, age_min=30, age_max=50)
    print(f"Participants aged 30-50: {age_range}")
    
    # Test 4: Count females with DTI data, age 25 or older
    complex = count_with_filters(engine, sex='1', has_dti=True, age_min=25)
    print(f"Females with DTI, age 25+: {complex}")
    
    print("\n✓ All filter tests complete!")