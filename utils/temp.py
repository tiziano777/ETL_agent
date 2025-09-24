import streamlit as st
import os
import gzip
import json
import re
import pyarrow.parquet as pq
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, Any, List, Union, Optional, Tuple, Callable
import jsonschema

from mappings.mapper import Mapper

BASE_PATH = "/Users/T.Finizzi/repo/ETL_agent/data"
PROCESSED_DATA_DIR = "/Users/T.Finizzi/repo/ETL_agent/data/output"

# Simulazione di funzioni non fornite nel prompt
def show_dataset_selection(st_obj, base_path):
    st_obj.header("1. Selezione del Dataset")
    st_obj.write(f"Percorso base: {base_path}")
    st_obj.session_state.selected_version = "v1"
    st_obj.session_state.selected_dataset_name = "example_dataset"
    st_obj.session_state.selected_subfolder = "data"
    st_obj.success(f"Dataset selezionato: {st_obj.session_state.selected_dataset_name}")
    if st_obj.button("Avanti"):
        st_obj.session_state.current_stage = "action_selection"
        st_obj.rerun()
def show_action_selection(st_obj):
    st_obj.header("2. Selezione Azione")
    st_obj.write("Scegli 'Crea Schema' o 'Scarica in Formato Standard'.")
    st_obj.session_state.action = "scarica"
    st_obj.success(f"Azione selezionata: {st_obj.session_state.action}")
    if st_obj.button("Avanti"):
        st_obj.session_state.current_stage = "select_target_schema"
        st_obj.rerun()
def show_select_target_schema(st_obj):
    st_obj.header("3. Seleziona Schema Target")
    st_obj.write("Scegli lo schema standard aziendale (simulato).")
    # Simula la selezione di uno schema e di un mapping
    st_obj.session_state.dst_schema = {
        "type": "object",
        "properties": {
            "record_id": {"type": ["string", "null"]},
            "text": {"type": ["string", "null"]},
            "metadata": {
                "type": "object",
                "properties": {
                    "source": {"type": ["string", "null"]},
                    "timestamp": {"type": ["string", "null"]}
                }
            }
        }
    }
    st_obj.session_state.mapper_mapping = [
        {"src_field": "unique_id", "target_field": "record_id", "transformation": "src"},
        {"src_field": "content.text", "target_field": "text", "transformation": "src"},
        {"src_field": "timestamp", "target_field": "metadata.timestamp", "transformation": "src"},
        {"src_field": "N/A", "target_field": "metadata.source", "transformation": "web_scrape"}
    ]
    st_obj.success("Schema e mapping caricati.")
    if st_obj.button("Avanti"):
        st_obj.session_state.current_stage = "mapping_results"
        st_obj.rerun()
def show_mapping_results(st_obj):
    st_obj.header("4. Risultati del Mapping")
    st_obj.info("Mapping generato e validato con successo (simulato).")
    st_obj.write("Pronto per l'elaborazione parallela del dataset completo.")
    if st_obj.button("Avvia Pipeline ETL"):
        st_obj.session_state.current_stage = "run_parallel_mapping"
        st_obj.rerun()
def load_dataset_samples(path, k=1):
    return [{"unique_id": "1", "content": {"text": "Hello World!"}, "timestamp": "2024-01-01T12:00:00Z"}]


# Funzioni di supporto per l'elaborazione parallela
def parse_input_path(input_path: str) -> List[str]:
    supported_extensions = ('.parquet', '.jsonl.gz')
    files_to_process = []
    if os.path.isfile(input_path):
        if input_path.endswith(supported_extensions): files_to_process.append(input_path)
    elif os.path.isdir(input_path):
        for root, _, files in os.walk(input_path):
            for filename in files:
                if filename.endswith(supported_extensions): files_to_process.append(os.path.join(root, filename))
    else: print(f"Errore: Il percorso di input '{input_path}' non è valido.")
    return files_to_process

def process_file(file_path: str, mapper_mapping: List, dst_schema: Dict[str, Any], output_path: str) -> Tuple[str, bool, int]:
    mapper = Mapper()
    output_filename = os.path.splitext(os.path.basename(file_path))[0]
    output_filepath = os.path.join(output_path, f"{output_filename}_mapped.jsonl")
    mapped_samples = []
    processed_count = 0
    try:
        if file_path.endswith('.parquet'):
            table = pq.read_table(file_path)
            for row in table.to_pydict():
                mapped_sample = mapper.apply_mapping(row, mapper_mapping, dst_schema)
                mapped_samples.append(mapped_sample)
                processed_count += 1
        elif file_path.endswith('.jsonl.gz'):
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                for line in f:
                    sample = json.loads(line)
                    mapped_sample = mapper.apply_mapping(sample, mapper_mapping, dst_schema)
                    mapped_samples.append(mapped_sample)
                    processed_count += 1
        with open(output_filepath, 'w', encoding='utf-8') as f:
            for sample in mapped_samples: f.write(json.dumps(sample, ensure_ascii=False) + '\n')
        return (os.path.basename(file_path), True, processed_count)
    except Exception as e:
        print(f"Errore durante l'elaborazione del file {file_path}: {e}")
        return (os.path.basename(file_path), False, processed_count)

def run_parallel_mapping(input_path: str, output_path: str, mapping: List, dst_schema: Dict[str, Any], progress_bar):
    files_to_process = parse_input_path(input_path)
    if not files_to_process:
        st.warning("Nessun file supportato trovato nel percorso di input.")
        return
    os.makedirs(output_path, exist_ok=True)
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = {executor.submit(process_file, file, mapping, dst_schema, output_path): file for file in files_to_process}
        total_files = len(files_to_process)
        successful_files = 0
        total_processed_samples = 0
        for i, future in enumerate(as_completed(futures)):
            filename, success, processed_count = future.result()
            progress_bar.progress((i + 1) / total_files)
            if success:
                successful_files += 1
                total_processed_samples += processed_count
            else:
                st.error(f"Elaborazione del file {filename} fallita.")
        st.success("Elaborazione Completata!")
        st.write(f"File elaborati con successo: {successful_files} / {total_files}")
        st.write(f"Campioni totali elaborati: {total_processed_samples}")
        st.write(f"I dati sono stati salvati in: `{output_path}`")
    

def run_parallel_mapping():
    st.header("5. Esecuzione Pipeline ETL")
    st.info("La pipeline sta elaborando i dati in parallelo. Potrebbe volerci del tempo per i dataset più grandi.")

    # Costruisci i percorsi basati sullo stato della sessione
    input_data_path = st.session_state.dataset_data
    
    # Crea il percorso di output seguendo la regola di logica
    # PROCESSED_DATA_DIR/version/dataset_name/subfolder
    output_data_path = os.path.join(
        PROCESSED_DATA_DIR,
        st.session_state.selected_version,
        st.session_state.selected_dataset_name,
        st.session_state.selected_subfolder
    )
    
    st.write(f"**Percorso di input:** `{input_data_path}`")
    st.write(f"**Percorso di output:** `{output_data_path}`")

    # Utilizza una progress bar per mostrare l'avanzamento
    progress_bar = st.progress(0.0)
    
    # Esegui la pipeline di mapping parallela
    run_parallel_mapping(
        input_data_path,
        output_data_path,
        st.session_state.mapper_mapping,
        st.session_state.dst_schema,
        progress_bar
    )
    st.button("Torna all'inizio", on_click=lambda: st.session_state.update(current_stage="dataset_selection", metadata_confirmed=False, pipeline_started=False, src_schema=None, dst_schema=None))


def main():
    st.title("ETL Dashboard")
    st.write("Guida passo-passo alla creazione dello schema per i tuoi dataset.")

    # Inizializzazione dello stato di navigazione
    if "current_stage" not in st.session_state:
        st.session_state.current_stage = "dataset_selection"
        st.session_state.selected_dataset_name = ""
        st.session_state.selected_subfolder = "" 
        st.session_state.metadata_confirmed = False
        st.session_state.pipeline_started = False
        st.session_state.src_schema = None
        st.session_state.dst_schema = None
        st.session_state.dataset_path = ""
        st.session_state.dataset_data = ""
        st.session_state.samples = []
        st.session_state.mapper_mapping = []

    # HOME
    if st.session_state.current_stage == "dataset_selection":
        show_dataset_selection(st, BASE_PATH)
    
    # ACTON SELECTION
    elif st.session_state.current_stage == "action_selection":
        show_action_selection(st)

    # METADATA EDITOR
    elif st.session_state.current_stage in ["metadata", "confirm_delete"]:
        if st.session_state.selected_version and st.session_state.selected_dataset_name and st.session_state.selected_subfolder:
            st.session_state.dataset_path = os.path.join(BASE_PATH, st.session_state.selected_version, st.session_state.selected_dataset_name)
            st.session_state.dataset_data = os.path.join(st.session_state.dataset_path, st.session_state.selected_subfolder)
            st.session_state.samples = load_dataset_samples(st.session_state.dataset_data, k=1)
        show_metadata_editor(st)

    # PIPE 2: Schema Extraction
    elif st.session_state.current_stage == "schema_extraction_options":
        show_schema_options(st)
    elif st.session_state.current_stage == "schema_extraction":
        show_schema_extraction(st, langfuse_handler)
    
    # PIPE 3: Mapping Generation
    elif st.session_state.current_stage == "select_target_schema":
        show_select_target_schema(st)
        
    elif st.session_state.current_stage == "mapping_generation":
        show_mapping_generation(st, langfuse_handler)
    
    elif st.session_state.current_stage == "mapping_results":
        show_mapping_results(st)

    # NUOVO STADIO PER L'ESECUZIONE PARALLELA
    elif st.session_state.current_stage == "run_parallel_mapping":
        run_parallel_mapping()

if __name__ == "__main__":
    main()
