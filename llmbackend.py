import json
import logging
import ollama
import streamlit as st
from sqlalchemy import text

from database_manager import DatabaseManager

class ArgoRAG:
    """
    Handles the Retrieval-Augmented Generation pipeline for ARGO data using a local LLM via Ollama.
    """
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self._check_ollama_setup()

    def _check_ollama_setup(self):
        """
        Checks if the Ollama service is running and if the 'phi3' model is available.
        """
        try:
            ollama.list()
            logging.info("Ollama service is running.")
        except Exception:
            logging.error("Ollama service is not running. Please start the Ollama application.")
            st.error("Could not connect to Ollama. Please ensure the Ollama application is running.")
            return

        try:
            model_list = ollama.list().get('models', [])
            models = [m.get('name') for m in model_list if m.get('name')]
            if not any("phi3" in m for m in models):
                logging.warning("Phi-3 model not found. Run 'ollama pull phi3' in your terminal.")
                st.warning("Ollama is running, but the Phi-3 model is not available. Please run `ollama pull phi3`.")
        except Exception as e:
            logging.error(f"Could not verify Ollama models: {e}")
            st.error("Could not verify Ollama models. Please check your setup.")

    def answer_question(self, question: str):
        """
        Answers a user's question by performing a RAG pipeline with a local LLM.
        """
        logging.info(f"Received question: {question}")

        # 1. Retrieve relevant documents from ChromaDB
        try:
            query_embedding = self.db_manager.embedding_model.encode(question).tolist()
            search_results = self.db_manager.chroma_collection.query(
                query_embeddings=[query_embedding], n_results=5
            )
            logging.info(f"Found {len(search_results['ids'][0])} relevant profiles from ChromaDB.")
        except Exception as e:
            logging.error(f"Error querying ChromaDB: {e}")
            return "Sorry, I couldn't search for relevant data in the vector database.", []

        if not search_results or not search_results['ids'][0]:
            return "I couldn't find any ARGO profiles relevant to your question.", []

        # 2. Fetch full data from MySQL using SQLAlchemy Engine
        profile_sql_ids = [meta['profile_id_sql'] for meta in search_results['metadatas'][0]]
        context_data = []
        try:
            with self.db_manager.mysql_engine.connect() as conn:
                for profile_id in profile_sql_ids:
                    query = text(f"""
                        SELECT p.profile_time, p.latitude, p.longitude, p.temperature, p.salinity, f.wmo_number
                        FROM argo_profiles p
                        JOIN argo_floats f ON p.float_id = f.float_id
                        WHERE p.profile_id = :profile_id
                    """)
                    query_result = conn.execute(query, {"profile_id": profile_id}).fetchone()
                    if query_result:
                        # Convert SQLAlchemy Row to a dictionary
                        context_data.append(dict(query_result._mapping))
            logging.info(f"Successfully fetched details for {len(context_data)} profiles from MySQL.")
        except Exception as e:
            logging.error(f"Error fetching data from MySQL: {e}")
            return "Sorry, I failed to retrieve the full data for the relevant profiles.", []

        # 3. Generate a response using the local LLM
        prompt = self._build_prompt(question, context_data)
        try:
            response = ollama.generate(model='phi3', prompt=prompt, stream=False)
            logging.info("Successfully generated a response from Phi-3.")
            return response['response'], context_data
        except Exception as e:
            logging.error(f"Error communicating with Ollama: {e}")
            return "Sorry, I am having trouble connecting to the local Ollama service.", context_data

    def _build_prompt(self, question: str, context_data: list) -> str:
        """
        Builds a detailed prompt for the local LLM.
        """
        context_str = "Here is some relevant oceanographic data from ARGO floats:\n\n"
        for i, item in enumerate(context_data, 1):
            temp_preview = json.loads(item['temperature'])[:5] if item.get('temperature') else 'N/A'
            sal_preview = json.loads(item['salinity'])[:5] if item.get('salinity') else 'N/A'
            context_str += f"--- Data Point {i} ---\n"
            context_str += f"Float WMO Number: {item.get('wmo_number', 'N/A')}\n"
            context_str += f"Date: {item.get('profile_time').strftime('%Y-%m-%d') if item.get('profile_time') else 'N/A'}\n"
            context_str += f"Location: {item.get('latitude', 0.0):.2f}, {item.get('longitude', 0.0):.2f}\n"
            context_str += f"Temperature readings (first 5): {temp_preview}\n"
            context_str += f"Salinity readings (first 5): {sal_preview}\n\n"

        prompt = f"""
        You are an expert oceanographic data analyst. Answer the user's question based *only* on the provided ARGO float data.
        If the context is insufficient, state that you cannot answer from the given data. Do not use external knowledge. Be concise.

        Context:
        {context_str}

        Question: "{question}"

        Answer:
        """
        return prompt