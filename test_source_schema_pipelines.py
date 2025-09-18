import streamlit as st
import uuid
import json
import os
import traceback
import yaml
import ast

from langgraph.types import Command
from langchain_google_genai import ChatGoogleGenerativeAI

from pipelines.source_schema_pipeline import create_pipeline

from states.src_schema_state import State

from nodes.src_schema_nodes.schema_node import SchemaNode
from nodes.src_schema_nodes.human_review_node import HumanReviewNode
from nodes.src_schema_nodes.validation_node import ValidationNode
from nodes.src_schema_nodes.schema_writer import SchemaWriter

from langfuse import Langfuse
from langfuse.langchain import CallbackHandler

import dotenv
dotenv.load_dotenv()
BASE_PATH = os.getenv("BASE_PATH", "")

langfuse = Langfuse( 
    public_key= os.environ.get('LANGFUSE_PUBLIC_KEY'),
    secret_key= os.environ.get('LANGFUSE_PRIVATE_KEY'), 
    host= os.environ.get('LANGFUSE_STRING_CONNECTION')
)
langfuse_handler = CallbackHandler()

MODEL_CONFIG = "./config/gemini2.0-flash.yml"
PROMPTS_PATH = "./config/prompts.yml"

with open(MODEL_CONFIG, "r", encoding="utf-8") as f:
    llmConfig = yaml.safe_load(f)
    api_key = os.environ.get("GEMINI_API_KEY")
    llmConfig["gemini_api_key"] = api_key
with open(PROMPTS_PATH, "r", encoding="utf-8") as f:
    prompts = yaml.safe_load(f)

geminiLLM = ChatGoogleGenerativeAI(
    model = llmConfig["model_name"],
    google_api_key = llmConfig["gemini_api_key"],
    temperature = llmConfig["temperature"],
    max_output_tokens = llmConfig["max_output_tokens"],
    top_p = llmConfig["top_p"],
    top_k = llmConfig.get("top_k", None),
)

# Pipeline creata una sola volta
if "graph" not in st.session_state:
    st.session_state.graph = create_pipeline(
        llm_node=SchemaNode(llm=geminiLLM, prompt=prompts["source_schema_extraction_prompt"]),
        human_node=HumanReviewNode(),
        validation_node=ValidationNode(),
        writer_node=SchemaWriter()
    )

def dataset_researcher(base_path):
    """Modulo di ricerca e selezione dataset/cartella."""
    if not base_path or not os.path.isdir(base_path):
        st.error("BASE_PATH non configurato correttamente in .env")
        return None
    folders = [f for f in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, f))]
    search = st.text_input("Cerca dataset/cartella")
    filtered_folders = [f for f in folders if search.lower() in f.lower()]
    selected_folder = st.selectbox("Seleziona una cartella", filtered_folders)
    if selected_folder:
        st.success(f"Hai selezionato: {selected_folder}")
    return selected_folder

def metadata_editor(dataset_path):
    import json
    import streamlit as st
    metadata_file = os.path.join(dataset_path, "metadata_0.json") if dataset_path else None
    # Load metadata fields
    try:
        with open("./metadata_0.json", "r", encoding="utf-8") as f:
            metadata_fields = json.load(f)
    except Exception:
        metadata_fields = ["_task","_size","_records","_link","_dataset_description"]
    # Load existing metadata if file exists
    metadata_dict = {}
    if metadata_file and os.path.exists(metadata_file):
        try:
            with open(metadata_file, "r", encoding="utf-8") as f:
                metadata_dict = json.load(f)
        except Exception:
            metadata_dict = {}
    st.subheader("Inserisci Metadata del Dataset")
    st.write("Seleziona un campo, inserisci il valore, aggiungi/modifica/elimina.")
    if "metadata_entries" not in st.session_state:
        st.session_state["metadata_entries"] = metadata_dict.copy() if metadata_dict else {}
    field = st.selectbox("Campo Metadata", metadata_fields)
    value = st.text_input("Valore per il campo selezionato", value=st.session_state["metadata_entries"].get(field, ""))
    if st.button("+ Aggiungi/Modifica campo"):
        st.session_state["metadata_entries"][field] = value
    # Show current metadata entries with edit/delete
    to_delete = []
    to_edit = None
    for k, v in list(st.session_state["metadata_entries"].items()):
        col1, col2, col3 = st.columns([3,1,1])
        col1.write(f"**{k}**: {v}")
        if col2.button("✏️", key=f"edit_{k}"):
            st.session_state["edit_field"] = k
            st.session_state["edit_value"] = v
            st.rerun()
        if col3.button("🗑️", key=f"delete_{k}"):
            to_delete.append(k)
    # Apply deletes after iteration
    if to_delete:
        for k in to_delete:
            st.session_state["metadata_entries"].pop(k)
        st.rerun()
    # Editing UI
    if "edit_field" in st.session_state:
        st.write(f"Modifica campo: {st.session_state['edit_field']}")
        new_value = st.text_input("Nuovo valore", value=st.session_state.get("edit_value", ""), key="edit_value_input")
        if st.button("Salva Modifica"):
            st.session_state["metadata_entries"][st.session_state["edit_field"]] = new_value
            del st.session_state["edit_field"]
            del st.session_state["edit_value"]
            st.rerun()
        if st.button("Annulla Modifica"):
            del st.session_state["edit_field"]
            del st.session_state["edit_value"]
            st.rerun()
    if st.button("Conferma Metadata"):
        # Save/merge metadata
        to_save = metadata_dict.copy()
        to_save.update(st.session_state["metadata_entries"])
        if metadata_file:
            with open(metadata_file, "w", encoding="utf-8") as f:
                json.dump(to_save, f, indent=2, ensure_ascii=False)
        st.success("Metadata salvati!")
        st.session_state["metadata_confirmed"] = True
        st.rerun()
    # Optionally hide section after confirmation
    if st.session_state.get("metadata_confirmed"):
        st.write("Sezione metadata completata.")
        # Load and return saved metadata
        if metadata_file and os.path.exists(metadata_file):
            try:
                with open(metadata_file, "r", encoding="utf-8") as f:
                    saved_metadata = json.load(f)
                return saved_metadata
            except Exception:
                return st.session_state["metadata_entries"]
        else:
            return st.session_state["metadata_entries"]
    return st.session_state["metadata_entries"]

def main():
    st.title("ETL Dashboard")
    st.base_path = BASE_PATH
    st.dataset_folder = dataset_researcher(st.base_path)
    st.dataset_path = os.path.join(st.base_path, st.dataset_folder) if st.dataset_folder else None
    if st.dataset_path:
        metadata_editor(st.dataset_path)


    if "thread_id" not in st.session_state:
        st.session_state.thread_id = str(uuid.uuid4())

    config = {
        "configurable": {"thread_id": st.session_state.thread_id},
        "callbacks": [langfuse_handler]
    }

    st.title("Step-by-step Dataset Schema Creation")

    st.sample_input = '''{"prompt": "Se 'caldo' sta a 'freddo' come 'giorno' sta a 'notte', a cosa sta 'alto' in relazione a 'basso'? Analizza la relazione di opposti.", "response": "Alto sta a basso, perché alto e basso sono opposti, così come caldo e freddo o giorno e notte."}'''

    if "pipeline_started" not in st.session_state:
        st.session_state.pipeline_started = False

    if not st.session_state.pipeline_started:
        if st.button("Avvia pipeline"):
            try:
                samples = json.loads(st.sample_input)
            except Exception as e:
                st.error(f"Errore parsing JSON: {e}")
                st.stop()

            init_state = State(
                samples=samples,
                accept_schema_generation=None,
                valid=None,
                generated_schema=None,
                feedback=None,
                output_path=os.path.join(st.dataset_path, "schema.json"),
            )

            result = st.session_state.graph.invoke(init_state, config=config)
            st.session_state.interrupt = result["__interrupt__"]
            st.session_state.state = result
            st.session_state.pipeline_started = True

    if "interrupt" in st.session_state:
        interrupt = st.session_state.interrupt

        # Normalizza: se è lista → prendi il primo elemento
        if isinstance(interrupt, list):
            interrupt = interrupt[0]

        st.subheader("Ultima risposta del modello")

        schema_str = interrupt.value.get("assistant_output", "{}")
        # Only try to parse if schema_str looks like JSON or dict
        if schema_str and (schema_str.strip().startswith('{') or schema_str.strip().startswith('[')):
            try:
                schema_dict = json.loads(schema_str)
                st.json(schema_dict)
            except Exception:
                try:
                    schema_dict = ast.literal_eval(schema_str)
                    st.json(schema_dict)
                except Exception:
                    print(traceback.format_exc())
                    st.write(schema_str)
        else:
            st.write(schema_str)

        st.subheader("📊 Samples forniti")
        for msg in interrupt.value.get("chat_history", []):
            if getattr(msg, "role", None) == "user":  # controlla che sia ChatMessage
                st.code(msg.content, language="json")

        st.subheader("📝 Istruzioni")
        st.write(interrupt.value.get("instructions", ""))

        st.subheader("Decisione umana")
        action = st.radio("Scegli azione:", ["break","continue","restart"])

        feedback_text = ""
        if action == "continue":
            feedback_text = st.text_area("Inserisci feedback testuale")

        if st.button("Invia decisione"):
            decision = {"action": action}
            if action == "continue":
                decision["feedback"] = feedback_text

            result2 = st.session_state.graph.invoke(Command(resume=decision), config=config)
            if "__interrupt__" in result2:
                st.session_state.interrupt = result2["__interrupt__"]
                st.session_state.state = result2
                st.rerun()
            else:
                st.success("Pipeline completata ✅")
                st.json(result2)

if __name__ == "__main__":
    main()
