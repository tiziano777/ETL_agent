from states.src_schema_state import  State
from typing import Dict, Any
from langgraph.types import Command
from langgraph.graph import END
from typing import Literal

import json
from json_repair import repair_json
import jsonschema 

class ValidationNode:
    def __init__(self):
        pass

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

    def __call__(self, state: State) -> Command[Literal["llm_node","writer_node"]]:
        
        print("Validating schema against samples...")
        last_response = state.chat_history[-1].content
        schema = self.extract_json(last_response)
        self.validator = jsonschema.Draft7Validator(schema)
        error_messages = []
        valid = True
        i=0

        for sample in state.samples:
            if isinstance(sample, Dict):
                i=i+1
                try:    
                    validation_result = self.validator.validate(schema, sample)
                    if validation_result:
                        print("Validation result: ", validation_result)
                        continue
                except jsonschema.exceptions.ValidationError as e:
                    valid = False
                    state.error_messages.append(f"Validation Error {str(i)} : {str(e.message)}")

        if valid:
            print("Schema validato con successo!")
            return Command(
                goto="writer_node", 
                update={
                "generated_schema": schema,
                "valid": True
            })
        else:
            print("Schema non valido. Errori di validazione:", error_messages)
            # take from validation result and config
            feedback_msg = str(error_messages)
            return Command(
                goto="llm_node", 
                update={
                "generated_schema": schema,
                "valid": False,
                "feedback": feedback_msg
            })
