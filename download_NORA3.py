import xarray as xr
import numpy as np
from siphon.catalog import TDSCatalog
import os
import pyproj
import time

# --- Target Coordinates ---
# NOTE: If these change, we must change target directory
TARGET_LAT = 58.2238  # Ljungskile Latitude
TARGET_LON = 11.9224  # Ljungskile Longitude

# --- Output Configuration ---
# Create the output directory relative to the script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(script_dir, "NORA3_subset_atm_ljungskile")
os.makedirs(OUTPUT_DIR, exist_ok=True)

CATALOG_URL = 'https://thredds.met.no/thredds/catalog/nora3_subset_atmos/atm_hourly_v2/catalog.xml'
BASE_URL = 'https://thredds.met.no/thredds/dodsC/nora3_subset_atmos/atm_hourly_v2'

def main():
    # URL for the THREDDS catalog
    catalog_url = 'https://thredds.met.no/thredds/catalog/nora3_subset_atmos/atm_hourly_v2/catalog.xml'
    catalog = TDSCatalog(catalog_url)
    all_dataset_names = list(catalog.datasets)
    filtered_list = [name for name in all_dataset_names if 'topo' not in name]
    
    for file in filtered_list:
        output_filename = os.path.join(OUTPUT_DIR, file)
        if not os.path.exists(output_filename):
            print(f'Downloading {file}')
            url = f'{BASE_URL}/{file}'

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

            

            point_ds.to_netcdf(output_filename)

            print(f'Sucessfully saved file {file}')
            print('Pausing for five seconds before next download')
            time.sleep(5)
        else:
            try:
                ds = xr.open_dataset(output_filename)
                print(f'{file} exists. Skipping')
            except OSError as e:
                print(f'{output_filename} is corrupt. Removing it...')
                os.remove(output_filename)

        

if __name__=='__main__':
    main()