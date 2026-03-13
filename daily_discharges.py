# Mar 2026
# Author: P. Barli

# Workflow to get daily discharges for year 2100 for the Guadalquivir catchment 


import os 
import zipfile 
import cdsapi

# Define the folder for the workflow
workflow_folder = 'FLOOD_RIVER_discharges'
os.makedirs(workflow_folder, exist_ok=True)

data_folder = os.path.join(workflow_folder, 'data')
os.makedirs(data_folder, exist_ok=True)

data_folder_grid = os.path.join(data_folder, 'EHYPEgrid')
os.makedirs(data_folder_grid, exist_ok=True)

# Data access parameters 
experiment = ["historical", "rcp_2_6", "rcp_4_5", "rcp_8_5"]
gcms = ["ec_earth", "ec_earth", "ec_earth"]
rcms = ["cclm4_8_17", "racmo22e", "rca4"] 
ens_members = ["r12i1p1", "r12i1p1", "r12i1p1"]

hydrological_models_catch = [
    "e_hypecatch_m00", "e_hypecatch_m01", "e_hypecatch_m02", "e_hypecatch_m03",
    "e_hypecatch_m04", "e_hypecatch_m05", "e_hypecatch_m06", "e_hypecatch_m07"
]
hydrological_models_grid = ["e_hypegrid", "vic_wur"]

year = ["2010", "2100"]

# initialize the API client 
client = cdsapi.Client()

file = os.path.join(data_folder_grid, 'download.zip')
dataset = "sis-hydrology-variables-derived-projections"
request = {
    "product_type": "essential_climate_variables",
    "variable": ["river_discharge"],
    "variable_type": "absolute_values",
    "time_aggregation": "daily",
    "experiment": ["historical"],
    "hydrological_model": ["e_hypegrid"],
    "rcm": "cclm4_8_17",
    "gcm": "ec_earth",
    "ensemble_member": ["r12i1p1"],
    "period": ["2000"]
}
client.retrieve(dataset, request, file)

# Unzip the file that was just downloaded, and remove the zip file
with zipfile.ZipFile(file, 'r') as zObject:
    zObject.extractall(path=data_folder_grid)
os.remove(file)