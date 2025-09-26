import dotenv
import os
import glob
import json
import time

dotenv.load_dotenv()
METADATA_PATH = os.getenv("METADATA_PATH")
METADATA_ARCHIVED_PATH = os.getenv("METADATA_ARCHIVED_PATH")


def show_metadata_backup(st):
    if "metadata_entries" in st.session_state:
        del st.session_state.metadata_entries

    st.header("Metadata Backup")
    if st.button("🏠 Torna alla Home"):
        st.session_state.current_stage = "action_selection"
        st.rerun()

    # Ottieni info dataset corrente
    version = st.session_state.selected_version
    dataset_name = st.session_state.selected_dataset_name
    subpath = st.session_state.selected_subpath
    if not (version and dataset_name and subpath):
        st.info("Seleziona prima un dataset per gestire i backup dei metadati.")
        return

    # Costruisci il nome base del file metadata
    base_filename = f"{version}__{dataset_name}__{subpath}"
    current_metadata_pattern = os.path.join(METADATA_PATH, f"{base_filename}__*.json")
    current_files = glob.glob(current_metadata_pattern)
    if not current_files:
        st.warning("Nessun file di metadati corrente trovato.")
        return
    current_metadata_file = max(current_files)
    # Carica il contenuto corrente
    with open(current_metadata_file, "r", encoding="utf-8") as f:
        current_metadata_json = f.read()
        current_metadata = json.loads(current_metadata_json)
    st.subheader("Metadati correnti")
    st.json(current_metadata)

    # Cerca versioni archiviate
    archived_pattern = os.path.join(METADATA_ARCHIVED_PATH, f"{base_filename}__*.json")
    archived_files = glob.glob(archived_pattern)
    archived_files = sorted(archived_files)
    if not archived_files:
        st.info("Nessuna versione archiviata disponibile.")
        return
    st.subheader("Versioni archiviate disponibili")
    archived_file_names = [os.path.basename(f) for f in archived_files]
    selected_archived = st.selectbox("Seleziona una versione archiviata da visualizzare:", archived_file_names)
    if selected_archived:
        archived_file_path = os.path.join(METADATA_ARCHIVED_PATH, selected_archived)
        with open(archived_file_path, "r", encoding="utf-8") as f:
            archived_json = f.read()
            archived_metadata = json.loads(archived_json)
        st.markdown("**Anteprima versione archiviata**")
        st.json(archived_metadata)
        if st.button("Ripristina questa versione"):
            with open(current_metadata_file, "w", encoding="utf-8") as f:
                json.dump(archived_metadata, f, indent=2, ensure_ascii=False)
            st.success(f"Versione archiviata '{selected_archived}' ripristinata come metadati correnti.")
            time.sleep(0.5)
            st.session_state.current_stage = "action_selection"
            st.rerun()

