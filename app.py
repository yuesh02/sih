import streamlit as st
import pandas as pd
from database_manager import DatabaseManager
from llmbackend import ArgoRAG

# --- Page Configuration ---
st.set_page_config(
    page_title="flowchat",
    page_icon="",
    layout="wide"
)

# --- App State Management ---
# Using the session state to store the chatbot and conversation history
if 'db_manager' not in st.session_state:
    st.session_state.db_manager = DatabaseManager()
    st.session_state.rag_chatbot = ArgoRAG(st.session_state.db_manager)
    st.session_state.messages = []

# --- UI Rendering ---
st.title("ARGO Float Data Explorer")
st.caption("Ask a question about oceanographic data and get answers from our AI assistant.")

# Display chat messages from history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if "data" in message:
            st.dataframe(message["data"])

# Accept user input
if prompt := st.chat_input("Show me salinity profiles near the equator in March 2023"):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    # Display user message in chat message container
    with st.chat_message("user"):
        st.markdown(prompt)

    # Display assistant response in chat message container
    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        message_placeholder.markdown("Thinking...")
        
        # Get response from the RAG pipeline
        response, retrieved_data = st.session_state.rag_chatbot.answer_question(prompt)
        
        message_placeholder.markdown(response)
        
        # Prepare data for display
        if retrieved_data:
            df = pd.DataFrame(retrieved_data)
            # Clean up the data for better display
            df_display = df[['wmo_number', 'profile_time', 'latitude', 'longitude']].copy()
            df_display['profile_time'] = pd.to_datetime(df_display['profile_time']).dt.strftime('%Y-%m-%d')
            st.dataframe(df_display)
            
            # Store the message and data
            assistant_message = {"role": "assistant", "content": response, "data": df_display}
        else:
            assistant_message = {"role": "assistant", "content": response}
            
        st.session_state.messages.append(assistant_message)
