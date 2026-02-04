import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st
import pandas as pd
from src.db.connection import get_engine
from src.db.queries import count_with_filters, get_participant_ids, get_participants_data

# Page configuration
st.set_page_config(
    page_title="Participant Inventory",
    page_icon="🧠",
    layout="wide"
)

# Title
st.title("🧠 Participant Inventory Dashboard")

# Sidebar for filters
st.sidebar.header("Filters")

# 1. Diagnosis
dx_display = st.sidebar.selectbox("Diagnosis", ["Any", "Control", "MDD"])
if dx_display == "Control":
    dx_filter = '0'
elif dx_display == "MDD":
    dx_filter = '1'
else:
    dx_filter = None

# 2. Age range
age_min = st.sidebar.number_input("Minimum Age", min_value=0, max_value=100, value=0)
age_max = st.sidebar.number_input("Maximum Age", min_value=0, max_value=100, value=100)

# 3. Sex
sex_display = st.sidebar.selectbox("Sex", ["Any", "Male", "Female"])
if sex_display == "Male":
    sex_filter = '0'
elif sex_display == "Female":
    sex_filter = '1'
else:
    sex_filter = None

# 4. Site
site_display = st.sidebar.selectbox("Site", ["Any"] + [f"Site {i}" for i in range(1, 21)])
if site_display == "Any":
    site_filter = None
else:
    # Extract number from "Site 1" -> "1"
    site_filter = site_display.split()[1]

# 5. Dataset availability
st.sidebar.subheader("Required Datasets")
has_dti = st.sidebar.checkbox("Has DTI")
has_cortical_thickness = st.sidebar.checkbox("Has Cortical Thickness")
has_subcortical_volumes = st.sidebar.checkbox("Has Subcortical Volumes")  
has_cortical_surface_area = st.sidebar.checkbox("Has Cortical Surface Area")

# 6. Clinical variables
st.sidebar.subheader("Clinical Variables")

# Age of onset range
age_onset_min = st.sidebar.number_input("Min Age of Onset", min_value=0, max_value=100, value=0, key="age_onset_min")
age_onset_max = st.sidebar.number_input("Max Age of Onset", min_value=0, max_value=100, value=100, key="age_onset_max")

# Episodes range
episodes_min = st.sidebar.number_input("Min Episodes", min_value=0, max_value=10, value=0, key="episodes_min")
episodes_max = st.sidebar.number_input("Max Episodes", min_value=0, max_value=10, value=10, key="episodes_max")

# 7. Symptom scales
st.sidebar.subheader("Symptom Scales")

# BDI range
bdi_min = st.sidebar.number_input("Min BDI Total", min_value=0, max_value=63, value=0, key="bdi_min")
bdi_max = st.sidebar.number_input("Max BDI Total", min_value=0, max_value=63, value=63, key="bdi_max")

# HDRS range
hdrs_min = st.sidebar.number_input("Min HDRS Total", min_value=0, max_value=52, value=0, key="hdrs_min")
hdrs_max = st.sidebar.number_input("Max HDRS Total", min_value=0, max_value=52, value=52, key="hdrs_max")

# MADRS range
madrs_min = st.sidebar.number_input("Min MADRS Total", min_value=0, max_value=60, value=0, key="madrs_min")
madrs_max = st.sidebar.number_input("Max MADRS Total", min_value=0, max_value=60, value=60, key="madrs_max")

# Build filters dictionary
filters = {}

# Demographics
if dx_filter is not None:
    filters['dx'] = dx_filter

if age_min > 0:
    filters['age_min'] = age_min
    
if age_max < 100:
    filters['age_max'] = age_max

if sex_filter is not None:
    filters['sex'] = sex_filter

if site_filter is not None:
    filters['site_id'] = site_filter

# Dataset availability (only add if checked)
if has_dti:
    filters['has_dti'] = True
    
if has_cortical_thickness:
    filters['has_cortical_thickness'] = True
    
if has_subcortical_volumes:
    filters['has_subcortical_volumes'] = True
    
if has_cortical_surface_area:
    filters['has_cortical_surface_area'] = True

# Clinical variables
if age_onset_min > 0:
    filters['age_of_onset_min'] = age_onset_min
    
if age_onset_max < 100:
    filters['age_of_onset_max'] = age_onset_max

if episodes_min > 0:
    filters['episodes_min'] = episodes_min
    
if episodes_max < 10:
    filters['episodes_max'] = episodes_max

# Symptom scales
if bdi_min > 0:
    filters['bdi_total_min'] = bdi_min
    
if bdi_max < 63:
    filters['bdi_total_max'] = bdi_max

if hdrs_min > 0:
    filters['hdrs_total_min'] = hdrs_min
    
if hdrs_max < 52:
    filters['hdrs_total_max'] = hdrs_max

if madrs_min > 0:
    filters['madrs_total_min'] = madrs_min
    
if madrs_max < 60:
    filters['madrs_total_max'] = madrs_max

# Get database engine
engine = get_engine()

# Query the database
count = count_with_filters(engine, **filters)

# Display the result
st.metric("Matching Participants", count)

# Export button
if st.button("Export Participant IDs"):
    # Get participant IDs
    ids = get_participant_ids(engine, **filters)
    
    # Convert to DataFrame
    df_export = pd.DataFrame({'participant_id': ids})
    
    # Convert to CSV
    csv = df_export.to_csv(index=False)
    
    # Download button
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name="participant_ids.csv",
        mime="text/csv"
    )

# Export button
if st.button("Export Participant Data"):
    # Get participant IDs
    data = get_participants_data(engine, **filters)
    
    # Convert to CSV
    csv = data.to_csv(index=False)
    
    # Download button
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name="participant_data.csv",
        mime="text/csv"
    )

# Visualizations section
st.header("Data Overview")

# Get all matching participants data
df_all = get_participants_data(engine, **filters)

# Only show charts if there are participants
if len(df_all) > 0:
    # Create two columns for side-by-side charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Participants by Diagnosis")
        # Count by diagnosis and map to readable labels
        dx_counts = df_all['dx'].value_counts()
        # Map values: 0 -> Control, 1 -> MDD
        dx_counts.index = dx_counts.index.map({'0': 'Control', '1': 'MDD'})
        st.bar_chart(dx_counts)
    
    with col2:
        st.subheader("Participants by Sex")
        # Count by sex and map to readable labels
        sex_counts = df_all['sex'].value_counts()
        # Map values: 0 -> Male, 1 -> Female
        sex_counts.index = sex_counts.index.map({'0': 'Male', '1': 'Female'})
        st.bar_chart(sex_counts)
    
    # Dataset availability chart
    st.subheader("Dataset Availability")
    dataset_counts = pd.DataFrame({
        'DTI': [df_all['has_dti'].sum()],
        'Cortical Thickness': [df_all['has_cortical_thickness'].sum()],
        'Subcortical Volumes': [df_all['has_subcortical_volumes'].sum()],
        'Cortical Surface Area': [df_all['has_cortical_surface_area'].sum()]
    })
    st.bar_chart(dataset_counts.T)
else:
    st.warning("No participants match the selected filters.")