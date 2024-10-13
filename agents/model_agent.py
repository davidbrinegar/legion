from transformers import CodeAgent, HfApiEngine
from tools.agent_tools import get_node_id, get_model_details, get_model_dependencies
import json

class ModelAgent:
    def __init__(self, model_name):
        self.model_name = model_name
        self.conversation_log = []

    def get_model_info(self):
        self.conversation_log.append(f"Retrieving information for model: {self.model_name}")
        
        node_id = get_node_id(self.model_name)
        self.conversation_log.append(f"Node ID: {node_id}")
        
        details = get_model_details(self.model_name)
        self.conversation_log.append("Model details retrieved")
        
        dependencies = get_model_dependencies(self.model_name)
        self.conversation_log.append(f"Dependencies: {dependencies}")
        
        # Parse the JSON string into a Python dictionary
        details_dict = json.loads(details) if details.startswith('{') else {"error": details}
        
        return {
            "node_id": node_id,
            "details": details_dict,
            "dependencies": dependencies
        }

    def get_conversation_log(self):
        return self.conversation_log

    # Add other methods as needed for model-specific operations
