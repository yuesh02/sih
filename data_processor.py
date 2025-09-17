import os
import xarray as xr
import pandas as pd
import numpy as np
import json
import logging
from tqdm import tqdm

from config import ARGO_DATA_DIR
from database_manager import DatabaseManager

class ArgoDataProcessor:
    """
    Processes ARGO NetCDF files, extracting data and using a DatabaseManager to store it.
    """
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def process_and_ingest(self):
        """
        Finds, processes, and ingests all ARGO .nc files from the specified directory.
        """
        nc_files = [os.path.join(ARGO_DATA_DIR, f) for f in os.listdir(ARGO_DATA_DIR) if f.endswith('.nc')]
        if not nc_files:
            logging.error(f"No NetCDF (.nc) files found in '{ARGO_DATA_DIR}'. Please check the path.")
            return

        logging.info(f"Found {len(nc_files)} NetCDF files to process.")

        for nc_file_path in tqdm(nc_files, desc="Processing ARGO files"):
            try:
                # Use decode_times=False to handle time conversion manually and avoid potential issues
                with xr.open_dataset(nc_file_path, decode_times=False) as ds:
                    wmo_number = int(ds['PLATFORM_NUMBER'].values[0].decode().strip())
                    project_name = ds.attrs.get('project_name', 'N/A')
                    platform_type = ds.attrs.get('platform_type', 'N/A')

                    float_id_db = self.db_manager.get_or_create_float(wmo_number, project_name, platform_type)
                    float_id = ds["PLATFORM_NUMBER"].values
                    float_id = str(float_id[0])
                    float_id = float_id[2:-1].strip()


                    # --- FIX for FutureWarning: Use .sizes instead of .dims ---
                    num_profiles = ds.sizes['N_PROF']
                    
                    for i in range(num_profiles):
                        self._process_single_profile(ds.isel(N_PROF=i), float_id_db, float_id)

            except Exception as e:
                logging.warning(f"Could not process file {os.path.basename(nc_file_path)}: {e}", exc_info=False) # Set exc_info to False for cleaner logs

    def _process_single_profile(self, profile_ds, float_id_db, float_id):
        """
        Processes a single profile, extracts data, and loads it into databases via the manager.
        """
        cycle_number = int(profile_ds['CYCLE_NUMBER'].values)

        logging.info(f"info 1 ........", exc_info=False)
        
        if self.db_manager.check_profile_exists(float_id_db, cycle_number):
            return

        lat = float(profile_ds['LATITUDE'].values)
        lon = float(profile_ds['LONGITUDE'].values)

        logging.info(f"info 2 ........", exc_info=False)
        
        # JULD is days since 1950-01-01, handle conversion manually
        base_date = pd.Timestamp("1950-01-01")
        profile_time = base_date + pd.to_timedelta(profile_ds['JULD'].values, unit='D')
        
        # --- FIX for JSON Serialization: Convert numpy.float32 to Python float ---
        def get_param(ds, var_name):
            if var_name in ds:
                # For each value 'x', explicitly cast it to a Python float()
                return [float(x) if not np.isnan(x) else None for x in ds[var_name].values.flatten()]
            return None
        
        logging.info(f"info 3 .......", exc_info=False)

        pressure = get_param(profile_ds, 'PRES')
        temperature = get_param(profile_ds, 'TEMP')
        salinity = get_param(profile_ds, 'PSAL')
        
        bgc_params = {}
        bgc_vars = ['DOXY', 'CHLA', 'BBP700', 'NITRATE']
        for var in bgc_vars:
            if var in profile_ds:
                bgc_params[var] = get_param(profile_ds, var)

        logging.info(f"info 4 .........", exc_info=False)

        profile_data = {
            "float_id": float_id_db, "cycle_number": cycle_number, "profile_time": profile_time.to_pydatetime(),
            "latitude": lat, "longitude": lon, 
            "pressure": json.dumps(pressure) if pressure is not None else None,
            "temperature": json.dumps(temperature) if temperature is not None else None, 
            "salinity": json.dumps(salinity) if salinity is not None else None,
            "bgc_params": json.dumps(bgc_params) if bgc_params else None
        }

        profile_id_db = self.db_manager.insert_profile(profile_data)
        
        self.db_manager.add_profile_to_chromadb(
           profile_id_db, float_id, cycle_number, profile_time.to_pydatetime(), lat, lon, bgc_params.keys(), pressure, temperature, salinity
        )
