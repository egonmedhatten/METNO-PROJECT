import os
import xarray as xr
from datetime import datetime, timedelta

script_dir = os.getcwd() # os.path.dirname(os.path.abspath(__file__))

# The directory where the downloader script saved the data, relative to this script
BASE_DATA_DIR = os.path.join(script_dir, "MEPS_archive_ljungskile")

# The directory where we will save the combined, lagged ensembles, relative to this script
OUTPUT_DIR = os.path.join(script_dir, "MEPS_archive_lagged_ljungskile")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# How many consecutive HOURLY runs to combine into one lagged ensemble.
# 3 is a good starting point.
NUM_RUNS_TO_LAG = 3
# The desired number of forecast steps in the final ensemble
TARGET_FORECAST_STEPS = 62 

def find_earliest_forecast(root_directory: str) -> datetime | None:
    earliest_time = None

    for dirpath, _, filenames in os.walk(root_directory):
        # We only care about directories that contain files
        if not filenames:
            continue
        
        try:
            # Split the path by the OS's separator ('/' or '\')
            parts = dirpath.split(os.sep)
            
            # Get the last 4 directory names for the time components
            year, month, day, hour = map(int, parts[-4:])
            
            current_time = datetime(year, month, day, hour)
            
            if earliest_time is None or current_time < earliest_time:
                earliest_time = current_time
                
        except (ValueError, IndexError):
            # Ignore paths that don't fit the Y/M/D/H structure
            continue
            
    return earliest_time

def find_latest_forecast(root_directory: str) -> datetime | None:
    """
    Finds the latest forecast time using os.walk.

    Args:
        root_directory: The path to the main forecast directory.

    Returns:
        A datetime object for the latest forecast, or None if none found.
    """
    latest_time = None

    for dirpath, _, filenames in os.walk(root_directory):
        # We only care about directories that actually contain forecast files
        if not filenames:
            continue
        
        try:
            # Split the path by the OS's separator ('/' or '\')
            parts = dirpath.split(os.sep)
            
            # Get the last 4 directory names for the time components
            year, month, day, hour = map(int, parts[-4:])
            
            current_time = datetime(year, month, day, hour)
            
            # The key change: check if the current time is GREATER than the latest found so far
            if latest_time is None or current_time > latest_time:
                latest_time = current_time
                
        except (ValueError, IndexError):
            # Ignore paths that don't fit the Y/M/D/H structure
            continue
            
    return latest_time


def build_lagged_ensemble(target_issuance_dt):
    all_run_datasets = []

    for i in range(NUM_RUNS_TO_LAG):
        run_dt = target_issuance_dt - timedelta(hours=i)
        run_dir_name = run_dt.strftime('%Y/%m/%d/%H')
        run_path = os.path.join(BASE_DATA_DIR, run_dir_name)
        members = [xr.load_dataset(f'{run_path}/{member}') for member in os.listdir(run_path)]
        cleaned_members = []
        for member in members:
            if not 'height6' in member.coords:
                cleaned_members.append(member)
            else:
                member = member.rename({'height6': 'height2'})
                cleaned_members.append(member)
        hour_runs = xr.concat(cleaned_members, dim='ensebmle_member')
        all_run_datasets.append(hour_runs)

    ensemble = xr.concat(all_run_datasets, dim='ensebmle_member').dropna(dim='time')
    ensemble['forecast_reference_time'] = max(ensemble.forecast_reference_time)

    return ensemble

def main():

    errors = []

    earliest_run = find_earliest_forecast(BASE_DATA_DIR)
    earliest_issuance_time = earliest_run + timedelta(hours=NUM_RUNS_TO_LAG)
    latest_run = find_latest_forecast(BASE_DATA_DIR)

    proceed = True
    target_issuance_dt = earliest_issuance_time

    while target_issuance_dt <= latest_run:
        # print(f'Building lagged ensemble for issuance time {target_issuance_dt}')
        time_str = target_issuance_dt.strftime('%Y%m%dT%HZ')
        try:
            if not os.path.exists(f'{OUTPUT_DIR}/mens_lagged_ensemble_{time_str}.nc'):
                print(f'Building lagged ensemble for issuance time {target_issuance_dt}')
                ensemble = build_lagged_ensemble(target_issuance_dt)
                
                ensemble.to_netcdf(f'{OUTPUT_DIR}/mens_lagged_ensemble_{time_str}.nc')
            else:
                print(f'Lagged ensemble for issuance time {target_issuance_dt} exists. Skipping.')
            target_issuance_dt += timedelta(hours=1)
        except OSError as e:
            print(f'Cannot build lagged ensemble for {target_issuance_dt}...')
            errors.append(e)
            target_issuance_dt += timedelta(hours=1)
    print('Done!')
    for e in errors:
        print(e)
        
if __name__=='__main__':
    main()
    

