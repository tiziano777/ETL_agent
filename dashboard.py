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

#from states.src_schema_state import State
#from states.mapping_schema_state import State as MappingState

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
from ui.mapping_generation_handler import show_select_target_schema, show_mapping_generation
from ui.final_results_handler import show_mapping_results

from utils.sample_reader import load_dataset_samples

import dotenv
dotenv.load_dotenv()
BASE_PATH = os.getenv("BASE_PATH", "")
  

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

    # Gestione della navigazione tra le sezioni con if/elif
    if st.session_state.current_stage == "dataset_selection":
        show_dataset_selection(st)
    
    elif st.session_state.current_stage in ["metadata", "confirm_delete"]:
        # Assicurati che il percorso del dataset sia sempre aggiornato
        if st.session_state.selected_folder and st.session_state.selected_subfolder:
            st.session_state.dataset_path = os.path.join(BASE_PATH, st.session_state.selected_folder)
            st.session_state.dataset_data = os.path.join(st.session_state.dataset_path, st.session_state.selected_subfolder)
            st.session_state.samples = load_dataset_samples(st.session_state.dataset_data, k=1)
            print(st.session_state.samples)
            print(type(st.session_state.samples))
        show_metadata_editor(st)
    
    elif st.session_state.current_stage == "schema_extraction_options":
        show_schema_options(st)
    
    elif st.session_state.current_stage == "schema_extraction":
        show_schema_extraction(st)
        
    elif st.session_state.current_stage == "select_target_schema":
        show_select_target_schema(st)
        
    elif st.session_state.current_stage == "mapping_generation":
        show_mapping_generation(st)
    
    elif st.session_state.current_stage == "mapping_results":
        show_mapping_results(st)

if __name__ == "__main__":
    main()