import streamlit as st
import uuid
import json
import os
import yaml
import ast
import traceback

from langchain_google_genai import ChatGoogleGenerativeAI

from pipelines.source_schema_pipeline import create_pipeline
from pipelines.mapping_schema_pipeline import create_pipeline as create_mapping_pipeline

# from states.src_schema_state import State
# from states.mapping_schema_state import State as MappingState

from nodes.src_schema_nodes.schema_node import SchemaNode
from nodes.src_schema_nodes.human_review_node import HumanReviewNode
from nodes.src_schema_nodes.validation_node import ValidationNode
from nodes.src_schema_nodes.schema_writer_node import SchemaWriter

from nodes.mapping_schema_nodes.mapping_node import MappingNode 
from nodes.mapping_schema_nodes.human_review_node import HumanReviewNode as MappingHumanReviewNode
from nodes.mapping_schema_nodes.validation_node import ValidationNode as MappingValidationNode
from nodes.mapping_schema_nodes.mapping_writer_node import MappingWriter 

from ui.metadata_handler import show_dataset_selection, show_metadata_editor
from ui.schema_extration_handler import show_schema_options, show_schema_extraction
from ui.mapping_generation_handler import show_select_target_schema, show_mapping_generation, show_mapping_results

from utils.sample_reader import load_dataset_samples

import dotenv
dotenv.load_dotenv()
BASE_PATH = os.getenv("BASE_PATH", "")

# --- Nuova funzione per la selezione dell'azione ---
def show_action_selection(st_app):
    st_app.header("Scegli l'Operazione")
    st_app.write("Quale operazione desideri eseguire sul tuo dataset?")
    
    col1, col2, col3 = st_app.columns(3)
    with col1:
        # Bottone per lanciare la pipeline di estrazione dello schema
        if st_app.button("Estrai Schema", use_container_width=True):
            st_app.session_state.current_stage = "schema_extraction_options"
            st_app.rerun()
    with col2:
        # Bottone per lanciare la pipeline di generazione del mapping
        if st_app.button("Genera Mapping", use_container_width=True):
            st_app.session_state.current_stage = "select_target_schema"
            st_app.rerun()
    with col3:
        # Bottone per tornare a modificare i metadati
        if st_app.button("Modifica Metadati", use_container_width=True):
            st_app.session_state.current_stage = "metadata"
            st_app.rerun()
# --- Fine nuova funzione ---


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

# Inizializzazione delle pipelines

if "src_schema_graph" not in st.session_state:
    st.session_state.src_schema_graph = create_pipeline(
        llm_node=SchemaNode(llm=geminiLLM, prompt=prompts["source_schema_extraction_prompt"], feedback_prompt=prompts["feedback_prompt"]),
        human_node=HumanReviewNode(),
        validation_node=ValidationNode(),
        writer_node=SchemaWriter()
    )
if "mapping_graph" not in st.session_state:
    st.session_state.mapping_graph = create_mapping_pipeline(
        llm_node=MappingNode(llm=geminiLLM, prompt=prompts["mapping_schema_prompt"], feedback_prompt=prompts["mapping_schema_feedback_prompt"]),
        human_node=MappingHumanReviewNode(),
        validation_node=MappingValidationNode(),
        writer_node=MappingWriter()
    )


def show_dataset_selection(st_app):
    st_app.header("1. Selezione Dataset")
    st_app.write("Scegli la cartella e la sottocartella del tuo dataset.")
    folders = [d for d in os.listdir(BASE_PATH) if os.path.isdir(os.path.join(BASE_PATH, d))]
    
    if "selected_folder" not in st_app.session_state:
        st_app.session_state.selected_folder = ""
        st_app.session_state.selected_subfolder = ""

    selected_folder = st_app.selectbox("Seleziona Cartella Principale", [""] + folders, 
                                        index=folders.index(st_app.session_state.selected_folder) + 1 if st_app.session_state.selected_folder else 0)

    if selected_folder:
        st_app.session_state.selected_folder = selected_folder
        subfolder_path = os.path.join(BASE_PATH, selected_folder)
        subfolders = [d for d in os.listdir(subfolder_path) if os.path.isdir(os.path.join(subfolder_path, d))]
        
        selected_subfolder = st_app.selectbox("Seleziona Sottocartella del Dataset", [""] + subfolders,
                                              index=subfolders.index(st_app.session_state.selected_subfolder) + 1 if st_app.session_state.selected_subfolder else 0)
        
        if selected_subfolder:
            if st_app.session_state.selected_subfolder != selected_subfolder:
                st_app.session_state.selected_subfolder = selected_subfolder
            if st_app.button("Conferma Selezione"):
                # Estrai subito i samples dopo la conferma
                dataset_path = os.path.join(BASE_PATH, selected_folder)
                dataset_data = os.path.join(dataset_path, selected_subfolder)
                st_app.session_state.dataset_path = dataset_path
                st_app.session_state.dataset_data = dataset_data
                st_app.session_state.samples = load_dataset_samples(dataset_data, k=1)
                if st_app.session_state.current_stage != "action_selection":
                    st_app.session_state.current_stage = "action_selection"
                    st_app.rerun()


# Funzione principale che gestisce la logica di navigazione
def main():
    st.title("ETL Dashboard")
    st.write("Guida passo-passo alla creazione dello schema per i tuoi dataset.")

    # Inizializzazione dello stato di navigazione
    if "current_stage" not in st.session_state:
        st.session_state.current_stage = "dataset_selection"
        st.session_state.selected_folder = ""
        st.session_state.selected_subfolder = "" 
        st.session_state.metadata_confirmed = False
        st.session_state.pipeline_started = False
        st.session_state.src_schema = None
        st.session_state.dst_schema = None

    # HOME
    if st.session_state.current_stage == "dataset_selection":
        show_dataset_selection(st)
    
    # ACTON SELECTION
    elif st.session_state.current_stage == "action_selection":
        show_action_selection(st)

    # METADATA EDITOR
    elif st.session_state.current_stage in ["metadata", "confirm_delete"]:
        if st.session_state.selected_folder and st.session_state.selected_subfolder:
            st.session_state.dataset_path = os.path.join(BASE_PATH, st.session_state.selected_folder)
            st.session_state.dataset_data = os.path.join(st.session_state.dataset_path, st.session_state.selected_subfolder)
            st.session_state.samples = load_dataset_samples(st.session_state.dataset_data, k=1)
        show_metadata_editor(st)

    # PIPE 2: Schema Extraction
    elif st.session_state.current_stage == "schema_extraction_options":
        show_schema_options(st)
    elif st.session_state.current_stage == "schema_extraction":
        show_schema_extraction(st)
    
    # PIPE 3: Mapping Generation
    elif st.session_state.current_stage == "select_target_schema":
        show_select_target_schema(st)
        
    elif st.session_state.current_stage == "mapping_generation":
        show_mapping_generation(st)
    
    elif st.session_state.current_stage == "mapping_results":
        show_mapping_results(st)

if __name__ == "__main__":
    main()
