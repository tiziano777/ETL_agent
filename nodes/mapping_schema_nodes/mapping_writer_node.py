import os
import json
from states.mapping_schema_state import State

class MappingWriter:
    def __init__(self, log_path: str = "logs/mapping_generation_log.json"):
        self.log_path = log_path
        os.makedirs(os.path.dirname(self.log_path), exist_ok=True)

    def __call__(self, state: State) -> State:
        if state.mapping and state.valid:
            # Path del metadata.json
            metadata_file = state.output_path
            os.makedirs(os.path.dirname(metadata_file), exist_ok=True)

            # Carica o crea il file metadata.json
            if os.path.exists(metadata_file):
                with open(metadata_file, "r", encoding="utf-8") as f:
                    metadata_json = json.load(f)
            else:
                metadata_json = {
                    "doc_id": '',
                    "metadata": {},
                    "src_schema": {},
                    "dst_schema_id": '',
                    "mapping": []
                }

            # Aggiorna la sezione mapping
            metadata_json["mapping"] = state.mapping

            # Scrivi il file aggiornato
            with open(metadata_file, "w", encoding="utf-8") as f:
                json.dump(metadata_json, f, indent=2, ensure_ascii=False)

            # Logga la generazione
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(state.model_dump(), ensure_ascii=False) + '\n')
        else:
            print("No valid mapping to write.")
            state.valid = False
            state.error_messages.append("No valid mapping to write.")
        return state