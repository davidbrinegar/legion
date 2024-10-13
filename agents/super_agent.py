import ollama
from agents.model_agent import ModelAgent
import json

class SuperAgent:
    def __init__(self, model_name='llama3.1'):
        self.model_name = model_name
        self.model_agents = {}  # Dictionary to store model agents
        self.conversation_log = []

    def _generate_text(self, prompt):
        response = ollama.generate(model=self.model_name, prompt=prompt)
        return response['response']

    def process_request(self, user_input):
        self.conversation_log = []  # Reset conversation log for new request
        self.conversation_log.append(f"User Input: {user_input}")

        # Use Ollama to understand the request and identify relevant models
        initial_prompt = f"""Examine this DBT request and identify the relevant model names:
        {user_input}
        Only return a JSON array of strings, containing only the model names. For example:
        ["model1", "model2"] Respond only with valid JSON. Do not write an introduction or summary."""
        
        initial_response = self._generate_text(initial_prompt)
        self.conversation_log.append(f"Initial Ollama Response: {initial_response}")
        
        # Extract model names from the initial response
        model_names = self._extract_model_names(initial_response)
        self.conversation_log.append(f"Extracted Model Names: {model_names}")
        
        # Create or retrieve model agents for each identified model
        model_info = {}
        for model_name in model_names:
            if model_name not in self.model_agents:
                self.model_agents[model_name] = ModelAgent(model_name)
            
            self.conversation_log.append(f"Querying ModelAgent for {model_name}")
            model_info[model_name] = self.model_agents[model_name].get_model_info()
            self.conversation_log.extend(self.model_agents[model_name].get_conversation_log())
        
        # Use Ollama to formulate a final response based on the gathered information
        final_prompt = f"""Based on this model information: {json.dumps(model_info, indent=2)}, 
        provide a concise answer to the user's request: {user_input}. 
        Focus on specific details from the model information if available."""
        
        final_response = self._generate_text(final_prompt)
        self.conversation_log.append(f"Final Ollama Response: {final_response}")
        
        return final_response, self.conversation_log, self.model_agents

    def _extract_model_names(self, response):
        try:
            # Find the first occurrence of a JSON array in the response
            start = response.find('[')
            end = response.rfind(']') + 1
            if start != -1 and end != -1:
                json_str = response[start:end]
                model_names = json.loads(json_str)
                if isinstance(model_names, list):
                    return model_names
            self.conversation_log.append("Error: Could not find a valid JSON array in the response")
            return []
        except json.JSONDecodeError:
            self.conversation_log.append("Error: Could not parse JSON from response")
            return []

    # Add other methods as needed for super agent functionality
