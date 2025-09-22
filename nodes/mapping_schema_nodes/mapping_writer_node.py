import os
import json
from states.mapping_schema_state import State

class MappingWriter:
    def __init__(self,log_path:str="logs/mapping_generation_log.json"):
        self.log_path = log_path
        os.makedirs(os.path.dirname(self.log_path), exist_ok=True)

    def __call__(self, state: State) -> State:
        if state.mapping and state.valid:
            with open(state.output_path, "w", encoding="utf-8") as f:
                json.dump(state.mapping, f, indent=2, ensure_ascii=False)
            
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(state.model_dump(), ensure_ascii=False) + '\n')
        else:
            print("No valid mapping to write.")
            state.valid = False
            state.error_messages.append("No valid mapping to write.")
        return state