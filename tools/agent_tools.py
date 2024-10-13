from transformers import tool
import sqlite3
from pathlib import Path
import json
import os

# Helper function to get the latest manifest database
def get_latest_manifest_db():
    manifest_dir = Path("agent_data/dbt/manifest")
    db_files = list(manifest_dir.glob("*.db"))
    if not db_files:
        raise FileNotFoundError("No manifest database files found.")
    
    # Sort the files by modification time, most recent first
    latest_db = max(db_files, key=os.path.getmtime)
    
    print(f"Using database: {latest_db}")  # For debugging
    return latest_db

@tool
def get_node_id(node_name: str) -> str:
    """
    Get the unique ID of a node given its name.

    Args:
        node_name: The name of the node to look up
    """
    try:
        db_path = get_latest_manifest_db()
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT unique_id FROM nodes WHERE name = ?", (node_name,))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        else:
            return f"No node found with name: {node_name}"
    except sqlite3.Error as e:
        return f"An error occurred: {str(e)}"
    finally:
        if conn:
            conn.close()

@tool
def get_model_details(model_name: str) -> str:
    """
    Get details of a specific model.

    Args:
        model_name: The name of the model to look up
    """
    try:
        db_path = get_latest_manifest_db()
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
        SELECT name, description, raw_code, compiled_code, config, fqn, tags, meta
        FROM nodes
        WHERE resource_type = 'model' AND name = ?
        """, (model_name,))
        result = cursor.fetchone()
        
        if result:
            columns = ["name", "description", "raw_code", "compiled_code", "config", "fqn", "tags", "meta"]
            return json.dumps(dict(zip(columns, result)), indent=2)
        else:
            return f"No model found with name: {model_name}"
    except sqlite3.Error as e:
        return f"An error occurred: {str(e)}"
    finally:
        if conn:
            conn.close()

@tool
def get_model_dependencies(model_name: str) -> str:
    """
    Get the dependencies of a specific model.

    Args:
        model_name: The name of the model to look up dependencies for
    """
    try:
        db_path = get_latest_manifest_db()
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
        SELECT related_unique_id
        FROM parent_map
        WHERE unique_id = (SELECT unique_id FROM nodes WHERE name = ? AND resource_type = 'model')
        """, (model_name,))
        results = cursor.fetchall()
        
        if results:
            dependencies = [row[0] for row in results]
            return json.dumps(dependencies, indent=2)
        else:
            return f"No dependencies found for model: {model_name}"
    except sqlite3.Error as e:
        return f"An error occurred: {str(e)}"
    finally:
        if conn:
            conn.close()

@tool
def execute_custom_query(query: str) -> str:
    """
    Execute a custom SQL query on the manifest database.

    Args:
        query: The SQL query to execute
    """
    try:
        db_path = get_latest_manifest_db()
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        if results:
            return json.dumps(results, indent=2)
        else:
            return "Query executed successfully, but no results were returned."
    except sqlite3.Error as e:
        return f"An error occurred: {str(e)}"
    finally:
        if conn:
            conn.close()

# You can add more tools here as needed
