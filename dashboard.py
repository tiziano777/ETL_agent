import streamlit as st
import uuid
import json
import os
import yaml
import ast
import traceback
from utils.sample_reader import load_dataset_samples

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

# Inizializzazione di Langfuse
langfuse = Langfuse( 
    public_key= os.environ.get('LANGFUSE_PUBLIC_KEY'),
    secret_key= os.environ.get('LANGFUSE_PRIVATE_KEY'), 
    host= os.environ.get('LANGFUSE_STRING_CONNECTION')
)
langfuse_handler = CallbackHandler()

# Configurazione del modello e dei prompt
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

# Inizializzazione della pipeline una sola volta
if "graph" not in st.session_state:
    st.session_state.graph = create_pipeline(
        llm_node=SchemaNode(llm=geminiLLM, prompt=prompts["source_schema_extraction_prompt"]),
        human_node=HumanReviewNode(),
        validation_node=ValidationNode(),
        writer_node=SchemaWriter()
    )

# --- Funzioni di visualizzazione delle sezioni ---

def show_dataset_selection():
    """Mostra la sezione di selezione del dataset."""
    st.subheader("1. Seleziona Dataset")
    if not BASE_PATH or not os.path.isdir(BASE_PATH):
        st.error("BASE_PATH non configurato correttamente nel file `.env`.")
        return
    
    folders = [f for f in os.listdir(BASE_PATH) if os.path.isdir(os.path.join(BASE_PATH, f))]
    search_query = st.text_input("Cerca dataset/cartella", key="dataset_search")
    
    filtered_folders = [f for f in folders if search_query.lower() in f.lower()]
    
    # L'utente seleziona la cartella
    st.session_state.selected_folder = st.selectbox(
        "Seleziona una cartella",
        options=[""] + filtered_folders,
        index=0,
        key="selected_folder_box"
    )
    
    # Aggiungi un pulsante per confermare la selezione
    if st.session_state.selected_folder:
        if st.button("✅ Conferma Selezione"):
            st.success(f"Hai selezionato: {st.session_state.selected_folder}")
            # L'aggiornamento dello stato avviene solo dopo la conferma
            st.session_state.current_stage = "metadata"
            st.rerun()

def show_metadata_editor():
    """Mostra la sezione di inserimento manuale dei metadati."""
    st.subheader("2. Inserisci Metadati del Dataset")
    st.write("Inserisci i metadati chiave per il tuo dataset.")
    
    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("⬅️ Torna alla selezione del Dataset"):
            st.session_state.current_stage = "dataset_selection"
            st.session_state.metadata_confirmed = False
            st.rerun()

    # Logica per l'editor dei metadati
    dataset_path = os.path.join(BASE_PATH, st.session_state.selected_folder)
    metadata_file = os.path.join(dataset_path, "metadata_0.json")

    # Campi predefiniti per i metadati
    try:
        with open("./metadata_0.json", "r", encoding="utf-8") as f:
            metadata_fields = json.load(f)
    except FileNotFoundError:
        metadata_fields = ["_task", "_size", "_records", "_link", "_dataset_description"]

    # Carica i metadati esistenti se il file esiste
    metadata_dict = {}
    if os.path.exists(metadata_file):
        try:
            with open(metadata_file, "r", encoding="utf-8") as f:
                metadata_dict = json.load(f)
        except Exception:
            st.warning("Impossibile caricare il file dei metadati esistente. Inizializzo con un dizionario vuoto.")
            metadata_dict = {}

    # Inizializza lo stato della sessione per i metadati
    if "metadata_entries" not in st.session_state or st.session_state.get("metadata_confirmed") == False:
        st.session_state["metadata_entries"] = metadata_dict.copy()

    # UI per aggiungere/modificare metadati
    new_field = st.selectbox("Campo Metadata", metadata_fields, key="new_field_select")
    new_value = st.text_input("Valore per il campo selezionato", key="new_value_input")

    if st.button("➕ Aggiungi/Modifica campo"):
        st.session_state["metadata_entries"][new_field] = new_value
        st.rerun()

    # Visualizza i metadati correnti
    st.markdown("---")
    st.subheader("Metadati Correnti")
    if st.session_state["metadata_entries"]:
        for key, value in st.session_state["metadata_entries"].items():
            col1_disp, col2_disp, col3_disp = st.columns([3, 1, 1])
            with col1_disp:
                st.write(f"**{key}**: {value}")
            with col2_disp:
                if st.button("✏️ Modifica", key=f"edit_btn_{key}"):
                    st.session_state["edit_key"] = key
                    st.session_state["edit_value"] = value
                    st.session_state.current_stage = "editing_metadata"
                    st.rerun()
            with col3_disp:
                if st.button("🗑️ Elimina", key=f"delete_btn_{key}"):
                    del st.session_state["metadata_entries"][key]
                    st.rerun()
    else:
        st.info("Nessun metadato inserito.")
    
    st.markdown("---")

    # UI per la conferma
    col3, col4 = st.columns([1, 1])
    with col3:
        if st.button("✅ Conferma Metadati"):
            try:
                # Salva i metadati nel file
                with open(metadata_file, "w", encoding="utf-8") as f:
                    json.dump(st.session_state["metadata_entries"], f, indent=2, ensure_ascii=False)
                st.success("Metadati salvati con successo!")
                st.session_state.metadata_confirmed = True
                st.session_state.current_stage = "schema_extraction_options"
                st.rerun()
            except Exception as e:
                st.error(f"Errore durante il salvataggio dei metadati: {e}")

def show_schema_options():
    """Mostra la sezione di estrazione o importazione dello schema."""
    st.subheader("3. Estrazione dello Schema")
    st.write("Vuoi estrarre lo schema dal dataset o ne hai già uno?")
    
    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("⬅️ Torna a Inserimento Metadati"):
            st.session_state.current_stage = "metadata"
            st.rerun()
    
    st.markdown("---")

    if st.button("🤖 Estrai Source Schema dal Dataset"):
        st.session_state.current_stage = "schema_extraction"
        st.session_state.pipeline_started = False # Reset della pipeline per avvio
        st.rerun()
        
    if st.button("📄 Ho già il Source Schema del dataset"):
        st.session_state.current_stage = "select_target_schema"
        st.rerun()

def show_schema_extraction():
    """Mostra la sezione di estrazione dello schema e gestisce la pipeline."""
    st.subheader("4. Estrazione Automatica dello Schema")
    st.write("La pipeline sta analizzando il tuo dataset per estrarre lo schema. Attendi il completamento o fornisci un feedback.")

    # Logica della pipeline
    if "pipeline_started" not in st.session_state or not st.session_state.pipeline_started:
        if "thread_id" not in st.session_state:
            st.session_state.thread_id = str(uuid.uuid4())

        # Preparazione dello stato iniziale e avvio
        try:
            st.session_state.samples = json.loads(json.dumps(st.session_state.samples))
        except Exception as e:
            st.error(f"Errore durante il parsing del JSON di esempio: {e}")
            return

        init_state = State(
            samples=st.session_state.samples,
            output_path=os.path.join(BASE_PATH, st.session_state.selected_folder, "schema.json"),
        )
        
        config = {
            "configurable": {"thread_id": st.session_state.thread_id},
            "callbacks": [langfuse_handler]
        }

        try:
            with st.spinner("Avvio della pipeline..."):
                result = st.session_state.graph.invoke(init_state, config=config)
            st.session_state.interrupt = result.get("__interrupt__")
            st.session_state.state = result
            st.session_state.pipeline_started = True
            st.rerun() # Rerun per mostrare l'UI di review
        except Exception as e:
            st.error(f"Errore durante l'avvio della pipeline: {e}")
            st.error(traceback.format_exc())

    if "interrupt" in st.session_state:
        interrupt = st.session_state.interrupt
        if isinstance(interrupt, list):
            interrupt = interrupt[0]

        st.markdown("---")
        st.subheader("Campioni del Dataset Utilizzati")
        st.write("Ecco i campioni di dati su cui si è basata la pipeline per generare lo schema.")
        try:
            st.json(st.session_state.samples)
        except Exception as e:
            st.error(f"Errore nella visualizzazione dei campioni: {e}")

        st.markdown("---")
        st.subheader("Esame dello Schema Generato")
        st.write("Qui puoi rivedere lo schema generato.")
        
        schema_str = interrupt.value.get("assistant_output", "{}")
        if schema_str and (schema_str.strip().startswith('{') or schema_str.strip().startswith('[')):
            try:
                schema_dict = json.loads(schema_str)
                st.json(schema_dict)
            except Exception:
                try:
                    schema_dict = ast.literal_eval(schema_str)
                    st.json(schema_dict)
                except Exception:
                    st.warning("Impossibile parsare lo schema come JSON/dizionario. Visualizzo come testo.")
                    st.write(schema_str)
        else:
            st.write(schema_str)

        st.markdown("---")

        feedback_text = ""
        st.subheader("Fornisci Feedback")
        feedback_text = st.text_area("Se vuoi ritentare la generazione, scrivi qui il tuo feedback per migliorarla:", value="", key="feedback_input")

        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("➡️ Prosegui"):
                decision = {"action": "break"}
                config = {"configurable": {"thread_id": st.session_state.thread_id}, "callbacks": [langfuse_handler]}
                try:
                    with st.spinner("Invio della decisione e ripresa della pipeline..."):
                        result2 = st.session_state.graph.invoke(Command(resume=decision), config=config)
                    st.success("✅ Pipeline completata!")
                    st.session_state.current_stage = "select_target_schema"
                    st.session_state.pipeline_started = False
                    st.json(result2)
                    st.balloons()
                    st.rerun()
                except Exception as e:
                    st.error(f"Errore durante la ripresa della pipeline: {e}")
                    st.error(traceback.format_exc())

        with col2:
            if st.button("🔄 Ritenta Generazione"):
                decision = {"action": "continue", "feedback": feedback_text}
                config = {"configurable": {"thread_id": st.session_state.thread_id}, "callbacks": [langfuse_handler]}
                try:
                    with st.spinner("Invio della decisione e ripresa della pipeline..."):
                        result2 = st.session_state.graph.invoke(Command(resume=decision), config=config)
                    
                    if "__interrupt__" in result2:
                        st.session_state.interrupt = result2["__interrupt__"]
                        st.session_state.state = result2
                        st.rerun()
                    else:
                        st.success("✅ Pipeline completata!")
                        st.session_state.current_stage = "select_target_schema"
                        st.session_state.pipeline_started = False
                        st.json(result2)
                        st.balloons()
                        st.rerun()

                except Exception as e:
                    st.error(f"Errore durante la ripresa della pipeline: {e}")
                    st.error(traceback.format_exc())

        with col3:
            if st.button("⬅️ Torna a Metadati"):
                st.session_state.current_stage = "metadata"
                st.session_state.pipeline_started = False
                st.session_state.interrupt = None # Pulizia
                st.rerun()
                
def show_select_target_schema():
    """Sezione dummy per la selezione del target schema."""
    st.subheader("5. Seleziona Target Schema")
    st.write("Questa è una sezione dummy. Qui potrai scegliere un target schema per la trasformazione dei dati. Attualmente, il processo termina qui.")
    
    if st.button("⬅️ Torna a Opzioni Schema", key="back_to_options_from_target_btn"):
        st.session_state.current_stage = "schema_extraction_options"
        st.session_state.pipeline_started = False
        st.rerun()

# --- Funzione principale per la gestione degli stati ---

def main():
    st.title("ETL Dashboard")
    st.write("Guida passo-passo alla creazione dello schema per i tuoi dataset.")

    # Inizializzazione dello stato di navigazione
    if "current_stage" not in st.session_state:
        st.session_state.current_stage = "dataset_selection"
        st.session_state.selected_folder = ""
        st.session_state.metadata_confirmed = False
        st.session_state.pipeline_started = False

    # Gestione della navigazione tra le sezioni con if/elif
    if st.session_state.current_stage == "dataset_selection":
        show_dataset_selection()
    
    elif st.session_state.current_stage == "metadata":
        # Assicurati che il percorso del dataset sia sempre aggiornato
        if st.session_state.selected_folder:
            st.session_state.dataset_path = os.path.join(BASE_PATH, st.session_state.selected_folder)
            st.session_state.dataset_data_subfolder = "data"
            st.session_state.dataset_data = os.path.join(st.session_state.dataset_path, st.session_state.dataset_data_subfolder)
            st.session_state.samples = load_dataset_samples(st.session_state.dataset_data, k=1)
        show_metadata_editor()
    
    elif st.session_state.current_stage == "schema_extraction_options":
        show_schema_options()
    
    elif st.session_state.current_stage == "schema_extraction":
        show_schema_extraction()
        
    elif st.session_state.current_stage == "select_target_schema":
        show_select_target_schema()

if __name__ == "__main__":
    main()