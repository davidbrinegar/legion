import streamlit as st
from pathlib import Path
import os
from tools.dbt_manifest_parser import parse_manifest
from agents.super_agent import SuperAgent

# Set up the root directory for saving databases
ROOT_DIR = Path("agent_data/dbt/manifest")
ROOT_DIR.mkdir(parents=True, exist_ok=True)

def dbt_manifest_parser():
    st.header("DBT Manifest Parser")
    
    st.write("This tool parses a DBT manifest file and stores the data in a SQLite database.")
    
    uploaded_file = st.file_uploader("Choose a manifest.json file", type="json")
    
    if uploaded_file is not None:
        # Save the uploaded file temporarily
        temp_path = Path("temp_manifest.json")
        with open(temp_path, "wb") as f:
            f.write(uploaded_file.getvalue())
        
        try:
            db_path = parse_manifest(temp_path, ROOT_DIR)
            st.success(f"Manifest parsed successfully! Database saved as: {db_path}")
            
            # Offer the database for download
            with open(db_path, "rb") as f:
                st.download_button(
                    label="Download SQLite Database",
                    data=f,
                    file_name=db_path.name,
                    mime="application/octet-stream"
                )
        except Exception as e:
            st.error(f"An error occurred while parsing the manifest: {str(e)}")
        finally:
            # Clean up the temporary file
            os.remove(temp_path)

def main():
    st.title("DBT Project Debugger")

    # Sidebar for tool selection
    tool = st.sidebar.selectbox(
        "Select a tool",
        ["DBT Manifest Parser", "DBT Manifest Query", "DBT Debugger"]
    )

    if tool == "DBT Manifest Parser":
        dbt_manifest_parser()
    elif tool == "DBT Manifest Query":
        dbt_manifest_query()
    elif tool == "DBT Debugger":
        user_input = st.text_area("Enter your DBT debugging request:")
        if st.button("Debug"):
            super_agent = SuperAgent()
            result = super_agent.process_request(user_input)
            st.write(result)

def dbt_manifest_query():
    st.header("DBT Manifest Query")
    
    model_name = st.selectbox("Select Ollama model", ["llama3.1", "codellama", "mistral"])
    user_input = st.text_area("Enter your DBT query:")
    
    if st.button("Query"):
        super_agent = SuperAgent(model_name=model_name)
        with st.spinner("Processing your request..."):
            result, conversation_log, model_agents = super_agent.process_request(user_input)
        
        # Display the final result
        st.subheader("Final Response:")
        st.write(result)
        
        # Display model information
        if model_agents:
            st.subheader("Model Information:")
            for model_name, model_info in model_agents.items():
                st.write(f"**{model_name}**")
                st.json(model_info.get_model_info())
        
        # Create a sidebar for conversation logs
        st.sidebar.header("Conversation Logs")
        
        # Add a selectbox to choose between SuperAgent and ModelAgents
        agent_choices = ["SuperAgent"] + list(model_agents.keys())
        if len(agent_choices) > 1:
            agent_choice = st.sidebar.selectbox("Select Agent Log", agent_choices)
            
            if agent_choice == "SuperAgent":
                for log in conversation_log:
                    st.sidebar.text(log)
            else:
                for log in model_agents[agent_choice].get_conversation_log():
                    st.sidebar.text(log)
        else:
            st.sidebar.text("No model agents were created.")
            for log in conversation_log:
                st.sidebar.text(log)

if __name__ == "__main__":
    main()
