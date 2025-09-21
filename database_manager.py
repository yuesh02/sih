import chromadb
import json
import requests

import logging
from sqlalchemy import create_engine, text
from sentence_transformers import SentenceTransformer
from langchain.text_splitter import RecursiveCharacterTextSplitter

# Import configurations from the config file
from config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME, CHROMA_PERSIST_DIR, CHROMA_COLLECTION_NAME

class DatabaseManager:
    """
    Manages all database interactions for both MySQL and ChromaDB.
    """
    def __init__(self):
        """
        Initializes database connections and the embedding model.
        """
        self.mysql_engine = self._setup_mysql_connection()
        self.chroma_client = chromadb.PersistentClient(path=CHROMA_PERSIST_DIR)
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        self.chroma_collection = self.chroma_client.get_or_create_collection(
            name=CHROMA_COLLECTION_NAME,
            metadata={"hnsw:space": "cosine"}
        )
        logging.info("Database connections and embedding model initialized.")

    def _setup_mysql_connection(self):
        """
        Establishes a connection to the MySQL database.
        """
        try:
            connection_url = f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
            engine = create_engine(connection_url)
            with engine.connect():
                logging.info(f"Successfully connected to MySQL database: '{DB_NAME}'")
            return engine
        except Exception as e:
            logging.error(f"Failed to connect to MySQL: {e}")
            logging.error("Please ensure MySQL is running and connection details are correct.")
            exit()

    def create_mysql_tables(self):
        """
        Creates the necessary tables in the MySQL database if they don't already exist.
        """
        create_floats_table_sql = """
        CREATE TABLE IF NOT EXISTS argo_floats (
            float_id INT AUTO_INCREMENT PRIMARY KEY,
            wmo_number INT UNIQUE NOT NULL,
            project_name VARCHAR(255),
            platform_type VARCHAR(255)
        );
        """
        create_profiles_table_sql = """
        CREATE TABLE IF NOT EXISTS argo_profiles (
            profile_id INT AUTO_INCREMENT PRIMARY KEY,
            float_id INT NOT NULL,
            cycle_number INT NOT NULL,
            profile_time DATETIME NOT NULL,
            latitude FLOAT NOT NULL,
            longitude FLOAT NOT NULL,
            pressure JSON,
            temperature JSON,
            salinity JSON,
            bgc_params JSON,
            FOREIGN KEY (float_id) REFERENCES argo_floats(float_id),
            UNIQUE KEY (float_id, cycle_number)
        );
        """
        try:
            with self.mysql_engine.connect() as conn:
                conn.execute(text(create_floats_table_sql))
                conn.execute(text(create_profiles_table_sql))
                conn.commit()
            logging.info("MySQL tables 'argo_floats' and 'argo_profiles' are ready.")
        except Exception as e:
            logging.error(f"Error creating MySQL tables: {e}")
            exit()

    def get_or_create_float(self, wmo_number, project_name, platform_type):
        """
        Retrieves a float's ID from the database or creates a new entry.
        """
        with self.mysql_engine.connect() as conn:
            result = conn.execute(text("SELECT float_id FROM argo_floats WHERE wmo_number = :wmo"), {"wmo": wmo_number}).fetchone()
            if result:
                return result[0]
            else:
                insert_sql = text("INSERT INTO argo_floats (wmo_number, project_name, platform_type) VALUES (:wmo, :proj, :platform)")
                result = conn.execute(insert_sql, {"wmo": wmo_number, "proj": project_name, "platform": platform_type})
                conn.commit()
                return result.lastrowid

    def check_profile_exists(self, float_id, cycle_number):
        """Checks if a specific profile already exists in the database."""
        with self.mysql_engine.connect() as conn:
            result = conn.execute(text("SELECT 1 FROM argo_profiles WHERE float_id = :fid AND cycle_number = :cn"), {"fid": float_id, "cn": cycle_number}).fetchone()
            return result is not None

    def insert_profile(self, profile_data):
        """Inserts a new profile into the MySQL database and returns its ID."""
        logging.info("to sql .................")
        insert_sql = text("""
            INSERT INTO argo_profiles (float_id, cycle_number, profile_time, latitude, longitude, pressure, temperature, salinity, bgc_params)
            VALUES (:float_id, :cycle_number, :profile_time, :latitude, :longitude, :pressure, :temperature, :salinity, :bgc_params)
        """)
        with self.mysql_engine.connect() as conn:
            result = conn.execute(insert_sql, profile_data)
            conn.commit()
            return result.lastrowid

    def add_profile_to_chromadb(self, profile_id_db, float_id_db, cycle, time, lat, lon, bgc_keys, pressure, temperature, salinity):
        """
        Generates a summary, creates an embedding, and adds a profile to ChromaDB.
        """
        logging.info("ading profile to chroma.............")
        url = "http://10.176.0.140:11434/api/generate"

        lmit_once = len(pressure)//2

        limit_arr = [0, lmit_once, -1]
        summary_text = ''


        for i in range(2):


                

            prompt =  f"""
    You are a scientific data summarizer. I will provide you with raw oceanographic data including depth, salinity, temperature, date, and location. Based on this data, generate a concise but informative paragraph that summarizes the key details. The summary should include:

    When and where the data was collected

    The recorded values of depth, salinity, and temperature

    A simple interpretation of what these values indicate about the ocean conditions at that time and location.

    Here is the raw data:
    cycle:{cycle},
    float id:{float_id_db},
    date and time: {time}
    location : [{lon}, {lat}],

    depth: {pressure[limit_arr[i]:limit_arr[i+1]]},
    temperature: {temperature[limit_arr[i]:limit_arr[i+1]]},
    salinity: {salinity[limit_arr[i]:limit_arr[i+1]]}

i need the detailed explanation of the given data. use all the creativity and explain it as lengthy as possible
    """
        
            print(prompt)
            print("\n\n\n\n")
            
            payload = {
        "model": "llama3.1:8b",
        "prompt":prompt
    }

            headers = {
                "Content-Type": "application/json"
            }

            response = requests.post(url, data=json.dumps(payload), headers=headers, stream=True)

        

            # Ollama streams output line by line (JSON objects)
            for line in response.iter_lines():
                if line:
                    data = json.loads(line.decode("utf-8"))
                    if "response" in data:
                        summary_text+=data["response"]
                    if data.get("done", False):
                        break

        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=250,
            chunk_overlap=50,
            length_function=len
        )
        chunks = text_splitter.split_text(summary_text)

        print(f"Total chunks created: {len(chunks)}")
        
        embedding = self.embedding_model.encode(summary_text).tolist()
        
        self.chroma_collection.add(
            ids=[f"profile_{profile_id_db}"],
            embeddings=[embedding],
            documents=[summary_text],
            metadatas=[{
                "profile_id_sql": profile_id_db,
                "float_id_sql": float_id_db,
                "latitude": lat,
                "longitude": lon,
                "date": time.strftime('%Y-%m-%d')
            }]
        )