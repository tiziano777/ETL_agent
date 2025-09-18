# Project Overview
This document describes the design and implementation of an ETL Data Platform. The platform is designed for a semi-automated, human-in-the-loop workflow to ensure data quality and process control. The pipeline orchestration is handled by LangGraph, while the user interface is built with Streamlit.

## DOCUMENTATION:
You must read #PLANNING.md file to achieve all the logica information of the project

## Key Components: Node-by-Node Implementation

1. NavigatorNode
This node scans directories to discover processable files. After discovery, the pipeline is paused to allow the user to manually select the desired file.

```python

from states.pipeline_state import PipelineState

class NavigatorNode:
    """Scans directories and discovers processable files with format validation.
    
    This node triggers a human interrupt to allow for manual file selection.
    """
    
    SUPPORTED_EXTENSIONS = {'.jsonl', '.parquet', '.csv', '.gz'}

    def __init__(self):
        pass
    
    def __call__(self, state: PipelineState) -> PipelineState:
        try:
            if not state.dataset_path:
                state.error_status.append("No dataset path provided")
                return state
                
            discovered_files = self._scan_directory(state.dataset_path)
            
            if not discovered_files:
                state.warnings.append("No processable files found in directory")
            
            state.discovered_files = discovered_files
            state.requires_human_review = True
            state.interrupt_message = "File discovery complete. Please select a file from the list to proceed."
            
        except Exception as e:
            state.error_status.append(f"NavigatorNode error: {str(e)}")
            # Log the error for observability
            
        return state
    
    def _scan_directory(self, path: str) -> List[str]:
        """Recursively scan directory for supported file formats."""
        processable_files = []
        for root, _, files in os.walk(path):
            for file in files:
                if Path(file).suffix.lower() in self.SUPPORTED_EXTENSIONS:
                    full_path = os.path.join(root, file)
                    if self._validate_file_accessibility(full_path):
                        processable_files.append(full_path)
        return processable_files
    
    def _validate_file_accessibility(self, filepath: str) -> bool:
        """Check if file is readable and not empty."""
        try:
            return os.path.isfile(filepath) and os.access(filepath, os.R_OK) and os.path.getsize(filepath) > 0
        except OSError:
            return False

```

2. SchemaInferenceNode
This node uses an LLM to infer the JSON schema from data samples. A conditional feedback loop is implemented to allow the user to correct the schema if necessary, restarting the inference process.

```python
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import JsonOutputParser

from states.pipeline_state import PipelineState

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
            # Log the error
            
        return state
    
    def _extract_samples(self, filepath: str) -> List[Dict[str, Any]]:
        """Extract K samples from dataset based on file type."""
        try:
            file_extension = Path(filepath).suffix.lower()
            # Omitted helper functions like _read_jsonl_samples for brevity.
            if file_extension == '.jsonl':
                return [] # Placeholder
            elif file_extension == '.csv':
                return [] # Placeholder
            elif file_extension == '.parquet':
                return [] # Placeholder
            else:
                return []
        except Exception as e:
            return []

    def _infer_schema_with_llm(self, samples: List[Dict], prompt_content: str) -> Dict:
        """Use LLM to infer JSON schema from samples."""
        try:
            prompt_template = PromptTemplate(
                template=prompt_content,
                input_variables=["samples", "sample_count"]
            )
            
            prompt = prompt_template.format(
                samples=json.dumps(samples[:5], indent=2),
                sample_count=len(samples)
            )
            response = self.llm.invoke(prompt)
            schema = self.json_parser.parse(response.content)
            return schema
        except Exception as e:
            return None
    
    def _validate_schema_against_samples(self, schema: Dict, samples: List[Dict]) -> Dict:
        """Validate inferred schema against sample data."""
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
            # Handle case where jsonschema is not installed
            pass
        return validation_results
```

3. TargetSchemaDecisionNode
This node allows the user to manually select a target schema from organizational templates or uses an LLM to auto-detect the most suitable one.

# Selects target schema from organization templates
```python
class TargetSchemaDecisionNode:
    def __call__(self, state: PipelineState) -> PipelineState:
        # Implementation to load available templates or call LLM for auto-detection
        state.target_schema = {} # Dummy
        state.target_schema_path = "/path/to/template.json" # Dummy
        return state
```

4. SchemaMappingNode
This node uses an LLM to generate the transformation mapping. A conditional feedback loop is implemented here as well. After the LLM's initial attempt, the pipeline pauses for human review. The user can then accept the mapping or provide feedback, which routes the process back for another LLM-driven attempt.
```python
class SchemaMappingNode:
    """Creates transformation mapping between schemas with a feedback loop."""
    def __call__(self, state: PipelineState) -> PipelineState:
        # LLM mapping generation logic
        
        # Check for user feedback for re-mapping
        if state.user_corrections and 'mapping' in state.user_corrections:
            # Use feedback to refine the next prompt
            pass
        
        # Validate the mapping
        
        state.requires_human_review = True
        state.interrupt_message = "Schema mapping complete. Please review and provide feedback for corrections."
        state.mapping_for_review = {} # Dummy
        
        return state
```

5. WriterNode
The final node that persists the processed metadata, schemas, and mapping files.

# Persists metadata, schemas, and mapping files
```python
class WriterNode:
    def __call__(self, state: PipelineState) -> PipelineState:
        # File persistence logic
        state.processing_complete = True
        return state
```
