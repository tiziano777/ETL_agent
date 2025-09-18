from states.src_schema_state import State
import os
import json
from json_repair import repair_json

class SchemaNode:
    def __init__(self, llm, prompt: str, feedback_prompt: str = None):
        self.llm = llm
        self.prompt = prompt
        self.feedback_prompt = feedback_prompt


    def extract_json(self, json_text: str) -> dict:
        """
        Ripara e deserializza l'output JSON generato da LLM. Restituisce una lista oppure {} in caso di errore.
        """
        try:
            #print("json text: ", json_text)
            repaired_text = repair_json(json_text)
            parsed_json = json.loads(repaired_text)

            if not isinstance(parsed_json, dict):
                raise ValueError("Parsed JSON is not a list")

            return parsed_json

        except Exception as e:
            return e

    def __call__(self, state: State) -> State:
        """
        LLM generates candidate JSON schema from K samples.
        Input: state with prompt + sample_data OR Chat history Iterations
        Output: state with generated schema in updated chat_history
        """
        # LLM riceve tutta la chat_history e aggiunge samples and actual schema as input
        samples_json = json.dumps(state.samples, indent=2)
        actual_schema_json = json.dumps(state.generated_schema, indent=2) if state.generated_schema else "not yet available"
        if not state.feedback:
            msg_content = self.prompt.format(samples=samples_json, actual_schema=actual_schema_json)
            state.chat_history.append({"role": "user", "content": msg_content})
        else:
            msg_content = self.prompt.format(samples=samples_json, actual_schema=actual_schema_json)
            msg_content += self.feedback_prompt.format(feedback=state.feedback)
            state.chat_history.append({"role": "user", "content": msg_content})
        
        response = self.llm.invoke(state.chat_history)
        assistant_msg = {"role": "assistant", "content": str(self.extract_json(response.content))}
        
        return {
            "chat_history": state.chat_history + [assistant_msg],
            "accept_schema_generation": None,
            "feedback": None,
            "valid": None
        }

