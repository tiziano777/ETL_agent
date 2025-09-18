import streamlit as st

import os
import traceback
import yaml

from langfuse import Langfuse
from langfuse.langchain import CallbackHandler
from langchain_google_genai import ChatGoogleGenerativeAI

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

'''
try:
        # Invocazione della pipeline con tracciamento Langfuse
        state = await pipeline.ainvoke({"dataset_path": dataset_path}, config={"callbacks": [langfuse_handler]})

        error_status = state.get('error_status',[])
        if error_status == []:
            print(f"Errore nello stato per query = {dataset_path} : {state['error_status']}")
            
        return state
                
    except Exception as e:
        print(traceback.format_exc())
        exit(1)
'''

def run_pipeline_1():
    pass
def run_pipeline_2():
    pass
def run_pipeline_3():
    pass

def main():
    st.title("ETL Dashboard")
    st.base_path = BASE_PATH
    st.dataset_folder = dataset_researcher(st.base_path)
    st.dataset_path = os.path.join(st.base_path, st.dataset_folder) if st.dataset_folder else None
    if st.dataset_path:
        metadata_editor(st.dataset_path)



if __name__ == "__main__":
    main()