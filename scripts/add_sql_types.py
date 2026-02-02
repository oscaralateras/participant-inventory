"""
Script to automatically add SQL types to variables.csv based on variable name patterns.
Run once to populate the sql_type column.
"""

import pandas as pd

# Load the variables CSV
df = pd.read_csv('schema/variables.csv')

def infer_sql_type(variable_name: str, dataset: str) -> str:
    """
    Infer SQL type based on variable name patterns.
    
    Args:
        variable_name: The canonical variable name
        dataset: The dataset this variable belongs to
        
    Returns:
        SQL type string (TEXT, INTEGER, or FLOAT)
    """
    variable_name = variable_name.lower()
    
    # Participant ID is always TEXT
    if variable_name == 'participant_id':
        return 'TEXT'
    
    # Categorical/text variables
    categorical_patterns = [
        'dx', 'sex', 'site_id', 'method', 'category', 
        'scale', 'ethnicity', 'race'
    ]
    if any(pattern in variable_name for pattern in categorical_patterns):
        return 'TEXT'
    
    # Integer variables (counts, episodes, individual symptom items)
    integer_patterns = [
        'age', 'episodes', 'recur', 'ad', 'rem', 'epi', 'adcur',
        'bdi_', 'hdrs_', 'ids_', 'qids_', 'madrs_', 'cesd_',  # Individual items
        'ctq_',  # CTQ subscales and items
        'education_years', 'iq'
    ]
    if any(pattern in variable_name for pattern in integer_patterns):
        # Exception: _total scores can have decimals sometimes
        if '_total' in variable_name:
            return 'INTEGER'  # Most total scores are integers, adjust if needed
        return 'INTEGER'
    
    # Float variables (brain metrics, continuous measurements)
    # DTI metrics (FA values)
    if dataset == 'dti':
        return 'FLOAT'
    
    # Cortical thickness, surface area, volumes
    if dataset in ['cortical_thickness', 'cortical_surface_area', 'subcortical_volumes']:
        return 'FLOAT'
    
    # BMI, SES scores, severity
    float_patterns = ['bmi', 'ses', 'severity', 'age_of_onset', 'icv', 'surfarea', 'thickness']
    if any(pattern in variable_name for pattern in float_patterns):
        return 'FLOAT'
    
    # Default to TEXT if uncertain (safest option)
    return 'TEXT'

# Apply the function to populate sql_type column
df['sql_type'] = df.apply(
    lambda row: infer_sql_type(row['variable_name'], row['dataset']), 
    axis=1
)

# Save the updated CSV
df.to_csv('schema/variables.csv', index=False)

print("✓ SQL types added to variables.csv")
print(f"✓ Processed {len(df)} variables across {df['dataset'].nunique()} datasets")

# Show a sample of the results
print("\nSample of assigned types:")
print(df[['dataset', 'variable_name', 'sql_type']].head(20))
