import os
import json
from states.src_schema_state import State

class SchemaWriter:
    def __init__(self,log_path:str="logs/schema_generation_log.json"):
        self.log_path = log_path
        os.makedirs(os.path.dirname(self.log_path), exist_ok=True)

    def __call__(self, state: State) -> State:
        if state.generated_schema and state.valid:
            with open(state.output_path, "w", encoding="utf-8") as f:
                json.dump(state.generated_schema, f, indent=2, ensure_ascii=False)
            with open(self.log_path, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2, ensure_ascii=False)
        else:
            print("No valid schema to write.")
            state.valid = False
            state.error_messagess.append("No valid schema to write.")
        return state