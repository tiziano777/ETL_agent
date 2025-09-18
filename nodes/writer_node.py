import os
import json
from langgraph_agents.ETL_agent.states.src_schema_state import PipelineState

class WriterNode:
    """Persists metadata, schemas, and mapping files."""
    def __init__(self, output_dir: str):
        self.output_dir = output_dir

    def __call__(self, state: PipelineState) -> PipelineState:
        try:
            dataset_name = state.dataset_name or "dataset"
            if state.metadata:
                self._write_json(f"{dataset_name}_metadata_0.json", state.metadata)
            if state.source_schema:
                self._write_json(f"{dataset_name}_schema.json", state.source_schema)
            if state.schema_mapping and state.target_schema_path:
                template_name = os.path.splitext(os.path.basename(state.target_schema_path))[0]
                self._write_json(f"{dataset_name}_{template_name}_mapping.json", state.schema_mapping)
            state.processing_complete = True
        except Exception as e:
            state.error_status.append(f"WriterNode error: {str(e)}")
        return state

    def _write_json(self, filename: str, data):
        path = os.path.join(self.output_dir, filename)
        try:
            with open(path, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception:
            pass