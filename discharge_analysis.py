# monthly river discharge analysis workflow provided by Climaxx (https://handbook.climaax.eu/notebooks/workflows/FLOODS/04_River_discharge_analysis/hazard_assessment_discharge_analysis.html)

#%%
import os 

import dask.diagnostics
import numpy as np
import xarray as xr
import geopandas as gpd
import pyogrio
import pooch 
from shapely.geometry import Point

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

#%%

# select area of interest 
locname = 'Guadalquivir'
loc = [-5.98, 37.52]  # longitude, latitude

# directory structure 
workflow_folder = 'FLOOD_RIVER_discharges'
data_folder = os.path.join(workflow_folder, 'data')

# define the directory for plots
plot_dir = os.path.join(workflow_folder, f'plots_{locname}')
os.makedirs(plot_dir, exist_ok=True)

# Folder of a local EHYPE dataset (if available, see the get_data notebook).
# If the folder doesn't exist, data is accessed from the CLIMAAX cloud storage.
data_folder_catch = os.path.join(data_folder, 'EHYPEcatch')

# load catchment dataset 
data_folder_subbasins = os.path.join(data_folder, 'EHYPE3_subbasins')
os.makedirs(data_folder_subbasins, exist_ok=True)

data_folder_grid = os.path.join(data_folder, 'EHYPEgrid')

#%%
# Catchment dataset and plotting

# retrieve the subbasins file from Zenodo 
pooch.retrieve(
    'doi:10.5281/zenodo.581451/EHYPE3_subbasins.zip',
    known_hash='ce1a48393adba92443fb99cb2651b7cfadf60af9e4cce0ad9cae8e7b52d3c684',
    fname='EHYPE3_subbasins.zip',
    path=data_folder,
    downloader=pooch.DOIDownloader(),
    processor=pooch.Unzip(extract_dir=os.path.basename(data_folder_subbasins))
)

# open the dataset of catchment contours as a GeoDataFrame variable: 
try:
    catchments = gpd.GeoDataFrame.from_file(
        os.path.join(data_folder_subbasins, 'EHYPE3_subbasins.shp')
    )
    print('Dataset loaded.')
except pyogrio.errors.DataSourceError:
    print(
        'Dataset with subbasin contours not found. '
        f'Please download it and place it in the folder {data_folder_subbasins}'
    )

catchments = catchments.set_index(catchments['SUBID'].astype(int))

# select catchment of interest based on `loc` 
point = Point((loc[0], loc[1]))

in_catchment = catchments.contains(point)
if not in_catchment.any():
    raise ValueError('The selected location is not within any catchment.')

catch_id = catchments[in_catchment].index.values[0]
print(f'Catchment ID in the E-HYPEcatch dataset: {catch_id}')

catchment = catchments.loc[catch_id]

# plot the catchment contour of the location of interest 
catchments['select'] = np.where(catchments.index == catch_id, 1, 0)

# select only the nearby catchments within radius of 1 degree (gdf.cx is a spatial indexer for GeoDataFrames)
catchments_sel = catchments.cx[(loc[0]-1):(loc[0]+1), (loc[1]-1):(loc[1]+1)]

#%% 
# make interactive plot 
fig = go.Figure()

fig.add_trace(go.Scattermapbox(
    lat=[loc[1]],  # Latitude coordinates
    lon=[loc[0]],   # Longitude coordinates
    mode='markers',
    marker=go.scattermapbox.Marker(size=14, color='red'),
    text=['Location of interest'],  # Labels for the points
    name=''
))

fig.add_trace(go.Choroplethmapbox(
    geojson=catchments_sel.to_geo_dict(),
    locations=catchments_sel.index,
    z=catchments_sel['select'],  
    hoverinfo='text',
    text=catchments_sel['SUBID'],  
    colorscale='RdPu',
    marker={'line': {'color': 'black', 'width': 1.5}},
    marker_opacity=0.2,  
    showscale=False
))

fig.update_layout(
    mapbox_center={'lat': loc[1], 'lon': loc[0]},
    mapbox_zoom=8,
    margin={'r': 0, 't': 0, 'l': 0, 'b': 0},
    mapbox={'style': 'open-street-map'})

fig.show()

fig.write_image(os.path.join(plot_dir, f'{locname}_location_catchment_map.png'))

#%%
# open the discharge dataset 

# extract metadata from filename and add it as new dimension to dataset 
def preprocess_monthly_mean(ds):
    filename = os.path.basename(ds.encoding['source'])
    _, _, _, catchmodel, gcm, scenario, _, rcm, _, time_period, _, _ = filename.split('_')
    ds['time'] = ds.time.dt.month
    return ds.expand_dims({
        'time_period': [time_period],
        'scenario': [scenario],
        'catchmodel': [catchmodel],
        'gcm_rcm': [f'{gcm}_{rcm}'],
    })
# open the dataset in chuncks with dask to enable lazy loading and parallel processing
ds_mon = xr.open_mfdataset(
    os.path.join(data_folder_catch, 'rdis_ymonmean_abs_E-HYPEcatch*-EUR-11_*_catch_v1.nc'), 
    preprocess=preprocess_monthly_mean, 
    chunks='auto'
)

# select data for sepcific catchment 
with dask.diagnostics.ProgressBar():
    ds_mon_sel = ds_mon.sel(id=catch_id).compute()

# save data for selected catchment to local disk 
ds_mon_sel.to_netcdf(os.path.join(data_folder, f'rdis_ymonmean_abs_E-HYPEcatch_allmodels_{catch_id}.nc'))

# mean of all catchment models 
ds_mon_sel = ds_mon_sel.mean(dim='catchmodel')

# %%
# Create a figure
fig = make_subplots(
    rows=1,
    cols=1,
    shared_xaxes=True,
    y_title='Monthly mean discharge [m3/s]',
    vertical_spacing=0.07,
    subplot_titles=[
        'Long-term (2071-2100)'
    ]
)

dashlist = ['dot', 'dash']
colorlist = px.colors.cyclical.mrybm[::2]

# plot long term projection of monthly mean river discharge
for tt, time_period in enumerate(ds_mon_sel.time_period.values):

    for ss, scen in enumerate(ds_mon_sel.scenario.values):
        for ii, gcm_rcm in enumerate(ds_mon_sel.gcm_rcm.values):
            fig.add_trace(go.Scatter(
                x=ds_mon_sel.time, y=ds_mon_sel.rdis_ymonmean.sel(gcm_rcm=gcm_rcm, scenario=scen, time_period=time_period), 
                mode='lines+markers', line={'color': colorlist[ii], 'dash': dashlist[ss]}, opacity=0.5, 
                name=f'{scen}, single GCM-RCM<br>(coloured lines)', legendgroup=f'{scen}, single GCM-RCM', 
                text=f'{gcm_rcm}', showlegend=(ii==0 and tt==0)
            ), row=tt+1, col=1)
        
        fig.add_trace(go.Scatter(
            x=ds_mon_sel.time, y=ds_mon_sel.rdis_ymonmean.sel(scenario=scen, time_period=time_period).median(dim='gcm_rcm'), 
            mode='lines+markers', line={'color': 'darkslategray', 'width': 3, 'dash': dashlist[ss]}, name=f'{scen}, median of GCM-RCMs', 
            text=f'{scen} ({time_period}), median of GCM-RCMs', showlegend=(tt==0)
        ), row=tt+1, col=1)

fig.update_yaxes(range=[0, np.nanmax(ds_mon_sel.rdis_ymonmean.values)*1.05])

# Customize layout
fig.update_layout(
    height=800, width=1100, 
    title_text=(
        f'<b>Monthly mean river discharges for the selected catchment</b> (ID {catch_id})<br>'
        'for different GCM-RCM combinations and averaged across the hydrological multi-model ensemble'
    ),
    showlegend=True,
    template='plotly_white',
)

# Show the figure
fig.show()

# Save figure
fig.write_image(os.path.join(plot_dir, f'{locname}_monthly_means.png'))
#%%
# open the daily discharge dataset 

# extract metadata from filename and add it as new dimension to dataset 
def preprocess_daily(ds):
    filename = os.path.basename(ds.encoding['source'])
    _, _, hydromodel, gcm, scenario, _, rcm, year, _, _ = filename.split('_')
    lat2d = ds['lat']
    return ds.expand_dims({
        'hydromodel': [hydromodel], 
        'gcm_rcm': [f'{gcm}_{rcm}'],
        'scenario': [scenario],
        'year': [year],
    }).assign_coords(lat=lat2d)

# open the dataset in chuncks with dask to enable lazy loading and parallel processing
ds_day = xr.open_mfdataset(
    os.path.join(data_folder_grid, 'rdis_day_E-HYPEgrid-EUR-11_*_grid5km_v1.nc'),
    preprocess=preprocess_daily,
    chunks='auto'
)

#%% 
# define the spatial dimension 
ds_day = ds_day.rio.set_spatial_dims(x_dim='x', y_dim='y')

# assign CRS if not already set
# they don't have a crs seems like 
# ds_try = ds_try.rio.write_crs("EPSG:4326")

# clip using the polygon
# clipped = ds_try.rio.clip(poly_series.geometry, poly_series.crs)

## for defining area of interest see https://deltares-research.github.io/IDP-handbook/tutorials/IDP-workbench/tutorials/co-data/access_stac_geotiff.html#processing-multiple-geotiffs-to-find-trends
# # select data for specific catchment 
# with dask.diagnostics.ProgressBar():
#     ds_day_sel = ds_day.sel(id=catch_id).compute()

# # save data for selected catchment to local disk 
# ds_mon_sel.to_netcdf(os.path.join(data_folder, f'rdis_ymonmean_abs_E-HYPEcatch_allmodels_{catch_id}.nc'))

# # mean of all catchment models 
# ds_mon_sel = ds_mon_sel.mean(dim='catchmodel')