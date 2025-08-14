import xarray as xr
from scipy.stats import ecdf
import numpy as np
from siphon.catalog import TDSCatalog
import warnings
import os
import datetime

# --- Target Coordinates ---
# NOTE: If these change, we must change target directory
TARGET_LAT = 58.2238  # Ljungskile Latitude
TARGET_LON = 11.9224  # Ljungskile Longitude

# --- Output Configuration ---
# Create the output directory relative to the script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(script_dir, "MEPS_latest_ljungskile")
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_valid_local_files(directory):
    """
    Checks a directory for valid NetCDF files that can be opened by xarray.
    Deletes any files that are corrupt or incomplete.
    """
    if not os.path.exists(directory):
        return set() # Return an empty set if the directory doesn't exist

    valid_files = set()
    for filename in os.listdir(directory):
        if filename.endswith('.nc'):
            file_path = os.path.join(directory, filename)
            try:
                # The 'with' statement ensures the file is closed after opening
                with xr.open_dataset(file_path):
                    pass # We just need to know if it opens without error
                valid_files.add(filename)
            except Exception as e:
                print(f"File directory/'{filename}' is corrupt or incomplete. Error: {e}")
                # Uncomment to remove corrupt file
                # os.remove(file_path)
    return valid_files


def download_data(target_lat, target_lon, save_dir):

    os.makedirs(save_dir, exist_ok=True)

    # Get a set of already downloaded and valid files
    valid_files = get_valid_local_files(save_dir)
    print(f"Found {len(valid_files)} valid local files.")


    # Suppress the HTML to XML warning for a cleaner output
    warnings.filterwarnings("ignore", category=UserWarning)

    # URL for the THREDDS catalog
    catalog_url = 'https://thredds.met.no/thredds/catalog/mepslatest/catalog.xml'
    catalog = TDSCatalog(catalog_url)
    all_dataset_names = list(catalog.datasets)

    # Filter out the files we want, excluding the 'latest' symlink directly
    search_string = 'meps_lagged_6_h_latest_2_5km_'
    filtered_list = [
            name for name in all_dataset_names 
            if search_string in name and name.endswith('.nc') and 'latest.nc' not in name
        ]

    for file in filtered_list[::-1]:
        if file not in valid_files:
            print(f'Fetching file: {file}')
            url = f'https://thredds.met.no/thredds/dodsC/mepslatest/{file}'
            try:
                with xr.open_dataset(url) as ds:
                    # Calculate the absolute difference between the target and all grid points
                    abs_diff = abs(ds.latitude - target_lat) + abs(ds.longitude - target_lon)
                    
                    # Find the 1D index of the minimum difference
                    minimum_index_1d = abs_diff.argmin()
                    
                    # Convert the 1D index to 2D indices for x and y
                    y_idx, x_idx = np.unravel_index(minimum_index_1d, ds.latitude.shape)

                    point_data = ds.isel(x=x_idx, y=y_idx)

                    point_data.to_netcdf(f"{save_dir}/{file}")

                    print(f"Successfully downloaded and saved {file}")
            except Exception as e:
                    # If a file fails, print the error and continue to the next
                    print(f"Could not process {file}. Skipping the file. Error: {e}")
                    continue
        else:
            print(f"{file} already exists and is valid. Skipping.")

if __name__=='__main__':

    # Get the current date and time
    now = datetime.datetime.now()

    # Format the time
    current_time = now.strftime("%Y-%m-%d %H:%M:%S")

    # Print the formatted time
    print(current_time)

    download_data(
         target_lat=TARGET_LAT,
         target_lon=TARGET_LON,
         save_dir=OUTPUT_DIR
    )
