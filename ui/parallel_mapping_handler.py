import os
from mappings.parallel_mapping_process import run_parallel_mapping
import json



def show_parallel_mapping(st, processed_data_dir, metadata_path):
    st.header("Esecuzione Pipeline ETL")
    st.info("La pipeline sta elaborando i dati in parallelo. Potrebbe volerci del tempo per i dataset più grandi.")
    
    input_data_path = st.session_state.dataset_data
    
    output_data_path = os.path.join(
        processed_data_dir,
        st.session_state.selected_version,
        st.session_state.selected_dataset_name,
        st.session_state.selected_subpath,
    )
    
    st.write(f"**Percorso di input:** `{input_data_path}`")
    st.write(f"**Percorso di output:** `{output_data_path}`")

    progress_bar = st.progress(0.0)
    
    # Callback per l'aggiornamento della barra di avanzamento
    def update_progress(value):
        progress_bar.progress(value)

    mapping_path= os.path.join(
        metadata_path,
        st.session_state.selected_version,
        st.session_state.selected_dataset_name,
        st.session_state.selected_subpath,
        "mapping.json",
    )
    if not os.path.exists(mapping_path):
        st.error(f"Mapping file not found at {mapping_path}. Please generate the mapping first.")
        return

    with open(mapping_path, "r") as f:
        st.session_state.mapping = json.load(f)
    
    st.session_state.mapping = st.session_state.mapping.get("mapping")
    if not st.session_state.mapping:
        st.error("Mapping is empty. Please generate the mapping first.")
        st.session_state.update(current_stage="action_selection", metadata_confirmed=False, pipeline_started=False, src_schema=None, dst_schema=None)


    results = run_parallel_mapping(
        input_data_path,
        output_data_path,
        st.session_state.mapping,
        st.session_state.dst_schema,
        update_progress
    )
    
    st.success("Elaborazione Completata!")
    st.write(f"File elaborati con successo: {results['successful_files']} / {results['total_files']}")
    st.write(f"Campioni totali elaborati: {results['total_processed_samples']}")
    st.write(f"I dati sono stati salvati in: `{output_data_path}`")

    st.button("Torna alla selezione del dataset", on_click=lambda: st.session_state.update(current_stage="dataset_selection", metadata_confirmed=False, pipeline_started=False, src_schema=None, dst_schema=None))
