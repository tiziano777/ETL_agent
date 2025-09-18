from typing import List, Dict, Any
from pathlib import Path
import json

from langchain_core.output_parsers import JsonOutputParser
from langgraph_agents.ETL_agent.states.src_schema_state import PipelineState

class SchemaInferenceNode:
    """Infers JSON schema from data samples using LLM with a feedback loop."""
    def __init__(self, llm, source_schema_prompt: str, max_samples: int = 5):
        self.llm = llm
        self.source_schema_prompt = source_schema_prompt
        self.max_samples = max_samples
        self.json_parser = JsonOutputParser()

    def __call__(self, state: PipelineState) -> PipelineState:
        try:
            if not state.selected_file:
                state.error_status.append("No file selected for schema inference")
                return state
            
            samples = self._extract_samples(state.selected_file)
            print(samples)
            
            if not samples:
                state.error_status.append("Could not extract samples from file")
                return state
            
            state.sample_data = samples
            prompt_with_feedback = self.source_schema_prompt

            if state.user_corrections and 'schema' in state.user_corrections:
                prompt_with_feedback += f"\n\nUser feedback to incorporate:\n{state.user_corrections['schema']}"

            inferred_schema = self._infer_schema_with_llm(samples, prompt_with_feedback)
            if inferred_schema:
                state.source_schema = inferred_schema
                validation_results = self._validate_schema_against_samples(inferred_schema, samples)
                state.schema_validation_results = validation_results
                state.requires_human_review = True
                state.interrupt_message = "Schema inference complete. Please review the results and provide feedback if needed."
                state.inferred_schema_for_review = inferred_schema
            else:
                state.error_status.append("Schema inference failed")
        except Exception as e:
            state.error_status.append(f"SchemaInferenceNode error: {str(e)}")
        return state

    def _extract_samples(self, filepath: str) -> List[Dict[str, Any]]:
        try:
            file_extension = Path(filepath).suffix.lower()
            # Implement file readers for each supported format
            if file_extension == '.jsonl':
                return self._read_jsonl_samples(filepath)
            elif file_extension == '.csv':
                return self._read_csv_samples(filepath)
            elif file_extension == '.parquet':
                return self._read_parquet_samples(filepath)
            else:
                return []
        except Exception:
            return []

    def _read_jsonl_samples(self, filepath: str) -> List[Dict[str, Any]]:
        samples = []
        try:
            with open(filepath, 'r') as f:
                for i, line in enumerate(f):
                    if i >= self.max_samples:
                        break
                    samples.append(json.loads(line))
        except Exception:
            pass
        return samples

    def _read_csv_samples(self, filepath: str) -> List[Dict[str, Any]]:
        import csv
        samples = []
        try:
            with open(filepath, 'r') as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    if i >= self.max_samples:
                        break
                    samples.append(row)
        except Exception:
            pass
        return samples

    def _read_parquet_samples(self, filepath: str) -> List[Dict[str, Any]]:
        samples = []
        try:
            import pandas as pd
            df = pd.read_parquet(filepath)
            samples = df.head(self.max_samples).to_dict(orient='records')
        except Exception:
            pass
        return samples

    def _infer_schema_with_llm(self, samples: List[Dict], prompt_content: str) -> Dict:
        try:
            prompt = self.source_schema_prompt.format(samples=json.dumps(samples, indent=2))
            response = self.llm.invoke(prompt)
            schema = self.json_parser.parse(response.content)
            return schema
        except Exception:
            return None

    def _validate_schema_against_samples(self, schema: Dict, samples: List[Dict]) -> Dict:
        validation_results = {
            "valid_samples": 0,
            "invalid_samples": 0,
            "errors": []
        }
        try:
            import jsonschema
            for i, sample in enumerate(samples[:10]):
                try:
                    jsonschema.validate(sample, schema)
                    validation_results["valid_samples"] += 1
                except jsonschema.ValidationError as e:
                    validation_results["invalid_samples"] += 1
                    validation_results["errors"].append(f"Sample {i}: {str(e)}")
        except ImportError:
            pass
        return validation_results