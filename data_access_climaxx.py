# workflow provided by Climaxx (https://handbook.climaax.eu/notebooks/workflows/FLOODS/04_River_discharge_analysis/hazard_assessment_get_data.html)

import os
import zipfile

import cdsapi

# Define the folder for the flood workflow
workflow_folder = 'FLOOD_RIVER_discharges'
os.makedirs(workflow_folder, exist_ok=True)

data_folder = os.path.join(workflow_folder, 'data')
os.makedirs(data_folder, exist_ok=True)

data_folder_catch = os.path.join(data_folder, 'EHYPEcatch')
os.makedirs(data_folder_catch, exist_ok=True)

# Data access parameters
# Using several model combinations helps to access the uncertainty range due to the different climate models in the river discharges data.
gcms = ["ec_earth", "hadgem2_es", "mpi_esm_lr", "ec_earth", "mpi_esm_lr", "hadgem2_es"]
rcms = ["cclm4_8_17", "racmo22e", "rca4", "racmo22e", "csc_remo2009", "rca4"]
ens_members = ['r12i1p1', 'r1i1p1', 'r1i1p1', 'r12i1p1', 'r1i1p1', 'r1i1p1']

hydrological_models = [
    "e_hypecatch_m00", "e_hypecatch_m01", "e_hypecatch_m02", "e_hypecatch_m03",
    "e_hypecatch_m04", "e_hypecatch_m05", "e_hypecatch_m06", "e_hypecatch_m07"
]

# initialize the API client 
client = cdsapi.Client()

# download monthly means of discharge for future periods of 2071-2100
for gcm, rcm, ens_member in zip(gcms, rcms, ens_members):
    for period in ["2071_2100"]:
        file = os.path.join(data_folder_catch, 'download.zip')
        dataset = "sis-hydrology-variables-derived-projections"
        request = {
            "product_type": "climate_impact_indicators",
            "variable": ["river_discharge"],
            "variable_type": "absolute_values",
            "time_aggregation": "monthly_mean",
            "experiment": ["rcp_4_5","rcp_8_5"],
            "hydrological_model": hydrological_models,
            "rcm": rcm,
            "gcm": gcm,
            "ensemble_member": ens_member,
            "period": period
        }
        client.retrieve(dataset, request, file)

        # Unzip the file that was just downloaded, and remove the zip file
        with zipfile.ZipFile(file, 'r') as zObject:
            zObject.extractall(path=data_folder_catch)
        os.remove(file)
