import os
import xarray as xr
import pandas as pd
from datetime import datetime, timedelta
import time
import requests
import numpy as np
import pyproj
from siphon.catalog import TDSCatalog


# TODO: Fix the directory structure. I jsut want the raw file names, since these are sufficient to identify member and time to build a lagged ensemble.

# --- Configuration ---
# The base URL for the new MEPS archive, using the OPeNDAP protocol
BASE_URL = "https://thredds.met.no/thredds/dodsC/meps25epsarchive"

# --- Target Coordinates ---
# NOTE: If these change, we must change target directory
TARGET_LAT = 58.2238  # Ljungskile Latitude
TARGET_LON = 11.9224  # Ljungskile Longitude

# --- Date Range for Historical Download ---
# Start date for the new archive format
START_TIME = datetime(2023, 10, 1, 0)
# The loop will run up to the most recent forecast cycle before today
END_TIME = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

# UPDATED: List of variables to extract
VARIABLES_TO_EXTRACT = [
    'forecast_reference_time',
    'projection_lambert',
    'air_temperature_2m',
    'relative_humidity_2m',
    'x_wind_10m',
    'y_wind_10m',
    'cloud_area_fraction',
    'air_pressure_at_sea_level',
    'precipitation_amount_acc',
    'snowfall_amount_acc',
    'wind_speed_of_gust',
    'fog_area_fraction'
 ]

# --- Output Configuration ---
# Create the output directory relative to the script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(script_dir, "MEPS_archive_ljungskile")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def get_urls(run_dt):
    """
    Constructs the list of OPeNDAP URLs for the .ncml aggregation files.
    """
    year = f"{run_dt.year}"
    month = f"{run_dt.month:02d}"
    day = f"{run_dt.day:02d}"
    hour = f"{run_dt.hour:02d}"
    
    catalog_url = f'https://thredds.met.no/thredds/catalog/meps25epsarchive/{year}/{month}/{day}/{hour}/catalog.xml'
    catalog = TDSCatalog(catalog_url)
    all_dataset_names = list(catalog.datasets)
    filtered_list = [name for name in all_dataset_names if '_sfc_' in name]
    print(f'Found {len(filtered_list)} members for {run_dt}')
    urls = [f'https://thredds.met.no/thredds/dodsC/meps25epsarchive/{year}/{month}/{day}/{hour}/{name}' for name in filtered_list]
    return urls
            
def extract_single_member_data(url):
    """
    Extracts data for a single member and returns its xarray.Dataset.
    """
    try:
        # Open the remote dataset directly using OPeNDAP
        ds = xr.open_dataset(url, engine="netcdf4")
        
        # Use the more accurate projection-based method
        crs_info = ds['projection_lambert'].attrs
        meps_crs = pyproj.CRS.from_cf(
            {
                "grid_mapping_name": crs_info['grid_mapping_name'],
                "standard_parallel": crs_info['standard_parallel'],
                "longitude_of_central_meridian": crs_info['longitude_of_central_meridian'],
                "latitude_of_projection_origin": crs_info['latitude_of_projection_origin'],
                "earth_radius": crs_info['earth_radius'],
            }
        )
        
        proj = pyproj.Proj.from_crs(4326, meps_crs, always_xy=True)
        X, Y = proj.transform(TARGET_LON, TARGET_LAT)
        
        x_idx = np.argmin(np.abs(ds.x.values - X))
        y_idx = np.argmin(np.abs(ds.y.values - Y))

        # Select data using the integer indices
        point_ds = ds.isel(y=y_idx, x=x_idx)
        
        # Keep only the variables we need to keep the dataset small
        point_ds_subset = point_ds[VARIABLES_TO_EXTRACT]#.isel(time=FORECAST_STEPS)
        
        return point_ds_subset

    except Exception as e:
        if isinstance(e, OSError) and "NetCDF: file not found" in str(e):
             print(f"URL not found (NetCDF error): {url}. Skipping member.")
        else:
            print(f"Could not process URL {url}. Error: {e}")
        return None
    
def make_save_dir(run_dt):
    year = f"{run_dt.year}"
    month = f"{run_dt.month:02d}"
    day = f"{run_dt.day:02d}"
    hour = f"{run_dt.hour:02d}"

    save_dir = os.path.join(OUTPUT_DIR, f"{year}/{month}/{day}/{hour}")
    try:
        os.makedirs(save_dir, exist_ok=False)
    except FileExistsError:
        print(f'{run_dt} exists. Check if it is populated. INTENTIONAL CRASH HERE')
        raise Exception
    return save_dir

def main():
    current_date = START_TIME
    while current_date < END_TIME:
        print(f'Processing {current_date}')
        save_dir = make_save_dir(current_date)
        urls = get_urls(run_dt=current_date)
        for url in urls:
            point_ds = extract_single_member_data(url=url)
            name = url.split('/')[-1]
            print(f'Downloading {name}')
            point_ds.to_netcdf(f'{save_dir}/{name}')

        current_date += timedelta(hours=1)
        print('Pausing a second before next hour')
        time.sleep(1)

if __name__ == "__main__":
    main()