import os
import json
from states.src_schema_state import State

class SchemaWriter:
    def __init__(self):
        pass

    def __call__(self, state: State) -> State:
        if state.generated_schema and state.valid:
            with open(state.output_path, "w", encoding="utf-8") as f:
                json.dump(state.generated_schema, f, indent=2, ensure_ascii=False)
        else:
            print("No valid schema to write.")
            state.valid = False
            state.error_messagess.append("No valid schema to write.")
        return state