# Mar 2026
# Author: P. Barli

# Workflow to get daily discharges for time period 2071-2080 for the Guadalquivir catchment 

import os 
import zipfile 
import cdsapi

# Define the folder for the flood workflow
# Define the folder for the flood workflow
workflow_folder = 'FLOOD_RIVER_discharges'
os.makedirs(workflow_folder, exist_ok=True)

data_folder = os.path.join(workflow_folder, 'data')
os.makedirs(data_folder, exist_ok=True)

data_folder_catch = os.path.join(data_folder, 'EHYPEcatch')
os.makedirs(data_folder_catch, exist_ok=True)
