import os
from utils.sample_reader import load_dataset_samples

def is_dir(path):
    """Verifica se un percorso è una directory."""
    return os.path.isdir(path)

def show_dataset_selection(st_app, BASE_PATH):
    st_app.header("1. Selezione Dataset")
    st_app.write("Scegli la versione, il nome del dataset e la sottocartella.")

    # Inizializza gli stati della sessione se non esistono
    if "selected_version" not in st_app.session_state:
        st_app.session_state.selected_version = ""
        st_app.session_state.selected_dataset_name = ""
        st_app.session_state.selected_subpath = ""
        st_app.session_state.search_version = ""
        st_app.session_state.search_dataset = ""
        st_app.session_state.search_subpath = ""

    # --- Sezione di selezione della Versione ---
    try:
        versions = [d for d in os.listdir(BASE_PATH) if is_dir(os.path.join(BASE_PATH, d))]
    except FileNotFoundError:
        st_app.error(f"Errore: la directory base '{BASE_PATH}' non è stata trovata.")
        return

    # Campo di ricerca per le versioni
    search_version = st_app.text_input("Cerca Versione", st_app.session_state.search_version, key="version_search_input")
    st_app.session_state.search_version = search_version
    filtered_versions = [v for v in versions if search_version.lower() in v.lower()]
    
    selected_version = st_app.selectbox(
        "Seleziona Versione",
        [""] + filtered_versions,
        index=filtered_versions.index(st_app.session_state.selected_version) + 1 if st_app.session_state.selected_version and st_app.session_state.selected_version in filtered_versions else 0
    )

    if selected_version and selected_version != st_app.session_state.selected_version:
        st_app.session_state.selected_version = selected_version
        # Resetta i livelli successivi quando la versione cambia
        st_app.session_state.selected_dataset_name = ""
        st_app.session_state.selected_subpath = ""
        st_app.session_state.search_dataset = ""
        st_app.session_state.search_subpath = ""
        st_app.rerun()

    # --- Sezione di selezione del Nome del Dataset ---
    if st_app.session_state.selected_version:
        version_path = os.path.join(BASE_PATH, st_app.session_state.selected_version)
        try:
            dataset_names = [d for d in os.listdir(version_path) if is_dir(os.path.join(version_path, d))]
        except FileNotFoundError:
            st_app.warning(f"La directory '{version_path}' non è stata trovata.")
            dataset_names = []

        search_dataset = st_app.text_input("Cerca Nome Dataset", st_app.session_state.search_dataset, key="dataset_search_input")
        st_app.session_state.search_dataset = search_dataset
        filtered_datasets = [d for d in dataset_names if search_dataset.lower() in d.lower()]

        selected_dataset_name = st_app.selectbox(
            "Seleziona Nome Dataset",
            [""] + filtered_datasets,
            index=filtered_datasets.index(st_app.session_state.selected_dataset_name) + 1 if st_app.session_state.selected_dataset_name and st_app.session_state.selected_dataset_name in filtered_datasets else 0
        )
        
        if selected_dataset_name and selected_dataset_name != st_app.session_state.selected_dataset_name:
            st_app.session_state.selected_dataset_name = selected_dataset_name
            # Resetta il livello successivo
            st_app.session_state.selected_subpath = ""
            st_app.session_state.search_subpath = ""
            st_app.rerun()

    # --- Sezione di selezione della Sottocartella ---
    if st_app.session_state.selected_version and st_app.session_state.selected_dataset_name:
        subpath_base = os.path.join(BASE_PATH, st_app.session_state.selected_version, st_app.session_state.selected_dataset_name)
        try:
            subpaths = [d for d in os.listdir(subpath_base) if is_dir(os.path.join(subpath_base, d))]
        except FileNotFoundError:
            st_app.warning(f"La directory '{subpath_base}' non è stata trovata.")
            subpaths = []

        search_subpath = st_app.text_input("Cerca Sottocartella", st_app.session_state.search_subpath, key="subpath_search_input")
        st_app.session_state.search_subpath = search_subpath
        filtered_subpaths = [s for s in subpaths if search_subpath.lower() in s.lower()]
        
        selected_subpath = st_app.selectbox(
            "Seleziona Sottocartella",
            [""] + filtered_subpaths,
            index=filtered_subpaths.index(st_app.session_state.selected_subpath) + 1 if st_app.session_state.selected_subpath and st_app.session_state.selected_subpath in filtered_subpaths else 0
        )
        
        if selected_subpath and selected_subpath != st_app.session_state.selected_subpath:
            st_app.session_state.selected_subpath = selected_subpath
            st_app.rerun()

    # --- Pulsante di conferma finale ---
    if st_app.session_state.selected_version and st_app.session_state.selected_dataset_name and st_app.session_state.selected_subpath:
        if st_app.button("Conferma Selezione"):
            dataset_path = os.path.join(BASE_PATH, st_app.session_state.selected_version, st_app.session_state.selected_dataset_name)
            dataset_data = os.path.join(dataset_path, st_app.session_state.selected_subpath)
            
            # Salva i percorsi finali nello stato della sessione
            st_app.session_state.dataset_path = dataset_path
            st_app.session_state.dataset_data = dataset_data
            
            # Esegui subito il caricamento dei dati
            st_app.session_state.samples = load_dataset_samples(dataset_data, k=1)
            
            if st_app.session_state.current_stage != "action_selection":
                st_app.session_state.current_stage = "action_selection"
                st_app.rerun()
