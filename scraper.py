import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin


# Base URL for the Ifremer data server
IFREMER_BASE_URL = "https://data-argo.ifremer.fr/dac/"

# The data center where the floats are managed (e.g., 'coriolis', 'aoml', 'bodc')
DATA_CENTER = 'incois'

# List of Argo Float IDs (WMO numbers) you want to scrape
FLOAT_IDS_TO_SCRAPE = [	 
"1902670",	 
"1902671",	 
"1902672",	 
"1902673",	 
"1902674",	 
"1902675",	 
"1902676",	 
"1902677",	 
"1902767",	 
"1902785",	 
"2900226",	 
"2900228",	 
"2900229",	 
"2900230",	 
"2900232",	 
"2900233",	 
"2900234",	 
"2900235",	 
"2900256",	 
# "2900257",	 
# "2900258",	 
# "2900259",	 
# "2900260",	 
# "2900261",	 
# "2900262",	 
# "2900263",	 
# "2900264",	 
# "2900265",	 
# "2900266",	 
# "2900267",	 
# "2900268",	 
# "2900269",	 
# "2900270",	 
# "2900271",	 
# "2900272",	 
# "2900273",	 
# "2900274",	 
# "2900275",	 
# "2900276",	 
# "2900335",	 
# "2900336",	 
# "2900337",	 
# "2900338",	 
# "2900339",	 
# "2900340",	 
]

# This must match the URL where your Flask app is running.
INGESTION_API_URL = "http://127.0.0.1:5000/files"

def fetch_and_upload_float_data(float_id: str):
    """
    Finds, downloads, and uploads all NetCDF profile files for a given float ID.
    
    Args:
        float_id (str): The World Meteorological Organization (WMO) number for the float.
    """
    print(f"\n--- Processing Float ID: {float_id} ---")
    
    #Construct the URL to the float's profile directory
    # Example: https://data-argo.ifremer.fr/dac/coriolis/1900121/profiles/
    profiles_url = urljoin(IFREMER_BASE_URL, f"{DATA_CENTER}/{float_id}/profiles/")
    
    print(f"Fetching file list from: {profiles_url}")
    
    try:
        #Get the directory listing page
        response = requests.get(profiles_url)
        response.raise_for_status()  # Raise an exception for bad status codes (like 404)
        
        # Parse the HTML to find all links
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a')
        
        nc_files_to_upload = []
        
        # Filter for only the core NetCDF data files (R*.nc or D*.nc)
        for link in links:
            filename = link.get('href')
            if (filename.startswith('R') or filename.startswith('D')) and filename.endswith('.nc'):
                file_url = urljoin(profiles_url, filename)
                
                # Download the file content into memory
                print(f"  Downloading {filename}...")
                file_response = requests.get(file_url)
                file_response.raise_for_status()
                
                # Prepare the file for multipart upload without saving it to disk first
                nc_files_to_upload.append(
                    ('files', (filename, file_response.content, 'application/x-netcdf'))
                )

        if not nc_files_to_upload:
            print(f"No NetCDF profile files (.nc) found for float {float_id}.")
            return

        # Send the batch of files to your ingestion API
        print(f"\nUploading {len(nc_files_to_upload)} files for float {float_id} to the API...")
        upload_response = requests.post(INGESTION_API_URL, files=nc_files_to_upload)
        upload_response.raise_for_status()
        
        print("Upload successful!")
        # print("API Response:", upload_response.json()) # Uncomment for debugging

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err} - Could not access data for float {float_id}.")
    except requests.exceptions.RequestException as req_err:
        print(f"A network error occurred: {req_err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    # Check if the API is reachable before starting
    try:
        requests.get(INGESTION_API_URL.rsplit('/', 1)[0], timeout=5)
        print("API is reachable. Proceeding with scraping.")
    except requests.exceptions.ConnectionError:
        print("\nCRITICAL ERROR: Could not connect to the ingestion API.")
        print(f"Please ensure your API server is running at: {INGESTION_API_URL}")
        exit() # Exit the script if the API isn't running
        
    for float_id in FLOAT_IDS_TO_SCRAPE:
        fetch_and_upload_float_data(float_id)
        
    print("\nScraping process finished.")