# ETL Datasets Platform

## Frameworks Documentation:

* See #docs/langchain-llms.txt #docs/langgraph-llms.txt #docs/langgraph-llms-full.txt folders to find documentation about Langgraph and Langchain.
* For observability, we use langfuse.
* Use Gemini API for first version see this example:

```python
  geminiLLM = ChatGoogleGenerativeAI(
          model = geminiConfig["model_name"],
          google_api_key = geminiConfig["gemini_api_key"],
          temperature = geminiConfig["temperature"],
          max_output_tokens = geminiConfig["max_output_tokens"],
          top_p = geminiConfig["top_p"],
          top_k = geminiConfig.get("top_k", None),
      )
```

## Project structure

docs/
  langgraph-llms.txt             # Markdown con link alla documentazione ufficiale 
  langgraph-llms-full.text       # Testo completo estratto dalla documentazione Langgraph
  langchain-llms.txt             # Extra: link/markdown per strumenti LangChain integrabili
config/                          # File YAML di configurazione (LLM conf, Database conf, http args, ecc.)
prompts/                         # YAML files with specific appliocation prompts
schema_templates/                # List of JSON schmas used as standard for our organization
images/                          # Containing jpeg images of agent architectures
data/                            # Dati in input/output, dataset
nodes/                           # Nodi LangGraph (classi Python: State -> State)
pipelines/                       # Grafi/pipeline: nodes + edges + policy/strategie
states/                          # Definizione pydantic/typing degli State
utils/                           # Optonal functions folder, examples: Client, LLMClient, interfacce, operazioni dati, helper...
dashboard.py                    # Esecuzione/test rapido di una pipeline con streamlit
requirements.txt                 # Dipendenze da installare
.gitignore                       # Regole git
.env                             # API KEY and secrets
.env.template                    # env file without secrets, usefull when run git clone operation
README.md                        # User manual
PLANNING.md                      # Project overview (this file)

## TASK: Modular ETL Pipelines using LangGraph

### CONTEXT:

* We have heterogeneous sources of raw datasets stored under a configurable `BASE_PATH`.

* User selects a dataset from available subfolders via dashboard (dropdown → folder names).

* Dashboard orchestrates two modular pipelines in sequence:

  1. **Schema Extraction & Validation Pipeline**
  2. **Schema Mapping Pipeline**

* Pipelines are semi-automatic, with human-in-the-loop interrupt steps.

* Focus on modularity, extensibility, and separation of concerns.

---

### WORKFLOW OVERVIEW

#### **Application-Level Preprocessing (outside LangGraph):**

1. User selects a dataset folder (scanned from `BASE_PATH`).
2. Application concatenates:

   * `DATASET_PATH = BASE_PATH + selected_folder`
   * `DATA_PATH = DATASET_PATH + /data`
3. First file inside `/data` is read with appropriate reader (supports `.jsonl`, `.csv`, `.gz`, `.parquet`), implemented as extensions of an **AbstractReader** class.
4. Read first **K samples** (user-defined K).

At this point, application holds:

* Path of dataset folder
* Path of data folder
* First **K samples**

This becomes input for **Pipeline 1**.

---

### PIPELINE 1: Schema Extraction & Validation

* **Input**: Dataset path, first K samples
* **Steps**:

  1. **LLM Schema Inference Node**

     * Prompted LLM generates candidate JSON schema.
  2. **Human Feedback Interrupt Node**

     * User can:

       * Continue with additional alignment instructions (CoT),
       * Modify schema manually,
       * Accept current schema.
  3. **Validation Node**

     * Validate candidate schema against the K samples.
     * If validation fails → loop back to inference node with error appended to prompt.
* **Output**: Validated source schema (`{dataset_name}_schema.json`).

---

### PIPELINE 2: Schema Mapping to Target

* **Input**:

  * Validated source schema (from Pipeline 1).
  * Target schema (user selects manually from `#schema_templates/`).

* **Steps**:

  1. **Mapping Generation Node**

     * LLM produces initial JSON mapping between source schema and target schema.
  2. **Human Feedback Interrupt Node**

     * User can:

       * Add alignment instructions,
       * Modify mapping manually,
       * Accept current mapping.
  3. **Validation Node**

     * Apply mapping to K samples.
     * Show transformed samples in dashboard.
     * If mapping fails or user rejects → loop back with feedback.

* **Output**: Validated mapping file (`{dataset_name}_{template_name}_mapping.json`).

---

### OUTPUT ARTIFACTS

For each dataset:

* `{dataset_name}_metadata_0.json`
* `{dataset_name}_schema.json`
* `{dataset_name}_{template_name}_mapping.json`

Stored in the dataset folder.

---

### TECHNICAL REQUIREMENTS

* **Streamlit**: Dashboard orchestration, dropdowns, feedback loops.
* **LangGraph**: Encapsulation of pipeline steps (LLM calls, validation, feedback loops).
* **Interrupts**: For human-in-the-loop checkpoints.
* **Async Processing**: For I/O and LLM calls.
* **Langfuse**: Observability.

---

### NODES (LangGraph)

#### 1. **Reader Abstraction (outside pipeline)**

* AbstractReader → CsvReader, JsonlReader, ParquetReader, GzReader
* Produces first K samples.

#### 2. **SchemaInferenceNode**

* Input: K samples
* Process: Prompted LLM → candidate schema
* Feedback loop with interrupt
* Output: Validated schema

#### 3. **TargetSchemaDecisionNode**

* Input: Source schema
* Process: Manual selection from templates (or optional LLM-assisted suggestion)
* Output: Path to target schema

#### 4. **SchemaMappingNode**

* Input: Source schema, Target schema
* Process: LLM generates mapping + feedback loop
* Output: Validated mapping file

#### 5. **WriterNode**

* Input: metadata, schema, mapping
* Process: Write to dataset folder
* Output: Final files

---

### IMPLEMENTATION ROADMAP

#### Sprint 1: Foundation

* Project structure, base classes, Reader abstraction

#### Sprint 2: Pipeline 1

* SchemaInferenceNode + feedback loop
* Validation node

#### Sprint 3: Pipeline 2

* TargetSchemaDecisionNode
* SchemaMappingNode + feedback loop

#### Sprint 4: Integration

* WriterNode
* Streamlit dashboard orchestration of the two pipelines

#### Sprint 5: Testing & Docs

* Unit + integration tests
* End-to-end pipeline tests with mock datasets
* Documentation
