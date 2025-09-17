import mysql.connector
import chromadb
import ollama
import json
from decimal import Decimal
 
# --- MySQL connection ---
def run_mysql_query(query):
    conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="shyam123",
            database="argo_ocean_data")
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute(query)
        results = cursor.fetchall()

        # Convert Decimal → float
        for row in results:
            for k, v in row.items():
                if isinstance(v, Decimal):
                    row[k] = float(v)

        return {"query": query, "results": results}
    except Exception as e:
        return {"error": str(e), "query": query}
    finally:
        cursor.close()
        conn.close()

# --- ChromaDB connection ---
client = chromadb.PersistentClient(path="./chroma_db_storage")
collection = client.get_collection("argo_float_profiles")

def query_chromadb(user_query):
    try:
        results = collection.query(
            query_texts=[user_query],
            n_results=2
        )
        return {"query": user_query, "result": results}
    except Exception as e:
        return {"query": user_query, "error": str(e)}

# --- Use Ollama (Gemma2 LLM) to decide ---
import ollama
import json

def ask_llm(user_prompt: str) -> str:
    """
    Asks a local LLM to decide which data source to use and generates the appropriate query.
    """
    # Note: ollama.chat is a simplified way to call the model. 
    # For more complex streaming or error handling, a library like 'requests' can be used.
    response = ollama.chat(
        model="gemma2", # Or another capable model like llama3
        messages=[{"role": "user", "content": f"""
You are an expert data assistant for an oceanographic database. Your job is to convert a user's question into a JSON object that can be used to query one of two databases.

## Available Data Sources:

1.  **MySQL Database**: This database stores structured, raw sensor data from Argo floats. Use it for questions that require precise numerical lookups, aggregations, or filtering based on specific values like ID, location, or time.
    - **`argo_floats` table**: Contains metadata about each float (`float_id`, `wmo_number`, `project_name`).
    - **`argo_profiles` table**: Contains detailed measurements for each profile (`profile_id`, `float_id`, `cycle_number`, `profile_time`, `latitude`, `longitude`). The columns `pressure`, `temperature`, and `salinity` are stored as JSON arrays.

2.  **ChromaDB (Vector Database)**: This database stores human-readable text summaries of each profile. Use it for conceptual or semantic questions, like "find profiles with unusual salinity" or "show me data from cold, deep water."

## Task
Based on the user's question below, you MUST output ONLY a single JSON object with the correct format for the chosen data source. Do not include any other text or markdown.

-   **For MySQL**, the format is:
    `{{"db": "mysql", "query": "A valid SQL query string that is safe to execute."}}`

-   **For ChromaDB**, the format is:
    `{{"db": "chromadb", "query": "A simple search text string that captures the user's intent."}}`

## User Question:
{user_prompt}
"""}]
    )
    
    # Extract and return the string content from the LLM's message
    return response["message"]["content"]

# --- Pipeline ---
import re

def process(user_prompt):
    llm_output = ask_llm(user_prompt)

    # --- Sanitize LLM output ---
    cleaned = llm_output.strip()

    # Remove ```json or ``` fences
    if cleaned.startswith("```"):
        cleaned = re.sub(r"```[a-zA-Z]*", "", cleaned)  # remove ```json or ```
        cleaned = cleaned.replace("```", "").strip()

    try:
        parsed = json.loads(cleaned)
    except Exception as e:
        return {"error": f"LLM did not return valid JSON: {e}", "raw": llm_output}

    if parsed["db"] == "mysql":
        return run_mysql_query(parsed["query"])
    elif parsed["db"] == "chromadb":
        return query_chromadb(parsed["query"])
    else:
        return {"error": "Unknown DB target", "raw": parsed}


# --- Test queries ---
if __name__ == "__main__":
    tests = [
        "Show me total presure by region",
        "Find papers about quantum computing"
    ]
    for t in tests:
        print(json.dumps(process(t), indent=2))
