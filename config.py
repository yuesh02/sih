# --- Database and File Path Configurations ---

# ARGO NetCDF files directory
# IMPORTANT: Update this path to the folder where you have stored the .nc files
ARGO_DATA_DIR = 'data'

# MySQL Database Connection Details
# IMPORTANT: Update these with your MySQL server details
DB_USER = 'root'
DB_PASSWORD = 'shyam123'
DB_HOST = 'localhost'
DB_PORT = '3306'
DB_NAME = 'argo_ocean_data'

# ChromaDB Configuration
CHROMA_PERSIST_DIR = 'chroma_db_storage'
CHROMA_COLLECTION_NAME = 'argo_float_profiles'
