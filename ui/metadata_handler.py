import os
import json
import glob
from datetime import datetime
import dotenv
import shutil

from utils.updater import archive_and_update_metadata, update_master_metadata

dotenv.load_dotenv()

BASE_PATH = os.getenv("BASE_PATH")
METADATA_PATH = os.getenv("METADATA_PATH")
METADATA_CONF = os.getenv("METADATA_CONF")

def find_metadata_files(st):
    """
    Trova il file di metadati più recente (tramite timestamp) per il dataset selezionato.
    Archivia in una sottocartella 'archived_metadata' qualsiasi file obsoleto trovato.
    """
    # Estrazione delle variabili dalla sessione Streamlit
    version = st.session_state.selected_version
    dataset_name = st.session_state.selected_dataset_name 
    subpath = st.session_state.selected_subpath

    # --- 1. Definizione del Pattern di Ricerca ---
    # Il pattern deve corrispondere ESATTAMENTE alla tua naming convention:
    # Esempio: v1__glaive__data__202509251540.json
    search_pattern = f"{version}__{dataset_name}__{subpath}__*.json"

    # 2. Cerca tutti i file che corrispondono
    full_pattern = os.path.join(METADATA_PATH, search_pattern)
    matching_files = glob.glob(full_pattern)
    
    if not matching_files:
        print(f"Errore: Nessun file di metadati trovato per {dataset_name}_{version} nel percorso {METADATA_PATH}")
        return None

    # Ordina i percorsi dei file: max() restituirà il file con il timestamp più alto
    latest_file_path = max(matching_files)

    # --- 3. Gestione e Archiviazione degli Obsoleti ---
    if len(matching_files) > 1: 
        print(f"Attenzione: Trovati {len(matching_files)} file per {dataset_name}_{version}. Archiviazione delle versioni più vecchie.")
        # Definisci e crea la directory di archiviazione
        archive_dir = os.path.join(METADATA_PATH, "archived_metadata")
        os.makedirs(archive_dir, exist_ok=True)
        
        for f_obsolete in matching_files:
            if f_obsolete != latest_file_path:
                
                file_name = os.path.basename(f_obsolete)
                new_archive_path = os.path.join(archive_dir, file_name)

                try:
                    os.rename(f_obsolete, new_archive_path) 
                    print(f"File obsoleto archiviato: {file_name}")
                except Exception as e:
                    print(f"Errore durante l'archiviazione di {file_name}: {e}")

    # 4. Restituisce il percorso del file più recente
    print(f"File di metadati corrente selezionato: {os.path.basename(latest_file_path)}")
    return latest_file_path

def show_metadata_editor(st):
    """Mostra la sezione di gestione e modifica dei metadati."""

    # Carica la lista dei campi metadati di sistema
    if os.path.exists(METADATA_CONF):
        with open(METADATA_CONF, "r", encoding="utf-8") as f:
            metadata_fields = json.load(f)
    else:
        metadata_fields = []

    st.subheader("Inserisci Metadati del Dataset")
    st.write("Inserisci e modifica i metadati chiave per il tuo dataset.")

    # Pulsante per tornare alla selezione
    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("⬅️ Torna alla selezione del Dataset"):
            st.session_state.selected_version = ""
            st.session_state.selected_dataset_name = ""
            del st.session_state.metadata_entries
            st.session_state.metadata_loaded = False
            st.session_state.metadata_confirmed = False
            st.session_state.current_stage = "dataset_selection"
            st.rerun()

    # Path
    dataset_path = os.path.join(BASE_PATH, st.session_state.selected_version, st.session_state.selected_dataset_name, st.session_state.selected_subpath)
    metadata_file = find_metadata_files(st)

    # =============================
    # Carica o crea il file metadata.json
    # =============================
    if metadata_file is None:
        metadata_file = os.path.join(METADATA_PATH,f"{st.session_state.selected_version}__{st.session_state.selected_dataset_name}__{st.session_state.selected_subpath}__{datetime.now().strftime('%Y%m%d%H%M')}.json")
        os.makedirs(os.path.dirname(metadata_file), exist_ok=True)
        metadata_json = {
            "doc_id": dataset_path,
            "metadata": {},
            "src_schema": {},
            "dst_schema_id": '',
            "mapping": []
        }
        with open(metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata_json, f, indent=2, ensure_ascii=False)
    else:
        with open(metadata_file, "r", encoding="utf-8") as f:
            metadata_json = json.load(f)
        # Assicura che tutte le chiavi ci siano
        for k in ["doc_id", "metadata", "src_schema", "dst_schema_id", "mapping"]:
            if k not in metadata_json:
                metadata_json[k] = {} if k != "mapping" else []
            if k == "dst_schema_id" and not metadata_json[k]:
                metadata_json[k] = ''

    metadata_dict = metadata_json["metadata"]

    # =============================
    # Inizializza lo stato solo al primo load
    # =============================
    if "metadata_entries" not in st.session_state:
        st.session_state.metadata_entries = metadata_dict.copy()
        st.session_state.metadata_loaded = True

    if "show_add_field" not in st.session_state:
        st.session_state.show_add_field = False
    if "show_delete_fields" not in st.session_state:
        st.session_state.show_delete_fields = False

    # =============================
    # Azioni su schema (campi disponibili)
    # =============================
    st.markdown("---")
    st.subheader("Metadati Disponibili (Schema)")
    st.write(metadata_fields)

    col_actions1, col_actions2 = st.columns([1, 1])
    with col_actions1:
        if st.button("➕ Aggiungi nuovo campo metadati"):
            st.session_state.show_add_field = True
            st.session_state.show_delete_fields = False

    with col_actions2:
        if st.button("🗑️ Elimina campi metadati"):
            st.session_state.show_delete_fields = True
            st.session_state.show_add_field = False

    # Aggiunta campo
    if st.session_state.show_add_field:
        st.markdown("### Aggiungi Nuovo Campo")
        new_field_name = st.text_input("Inserisci il nome del nuovo campo:", key="new_field_name_input")
        if st.button("Invia nuovo campo"):
            if new_field_name:
                if not new_field_name.startswith('_'):
                    new_field_name = "_" + new_field_name
                try:
                    if new_field_name not in metadata_fields:
                        metadata_fields.append(new_field_name)
                        with open(METADATA_CONF, "w", encoding="utf-8") as f:
                            json.dump(metadata_fields, f, indent=2, ensure_ascii=False)
                        st.success(f"Campo '{new_field_name}' aggiunto con successo!")
                        st.session_state.show_add_field = False
                        st.rerun()
                    else:
                        st.warning(f"Il campo '{new_field_name}' esiste già.")
                except Exception as e:
                    st.error(f"Errore durante l'aggiunta del campo: {e}")
            else:
                st.warning("Per favore, inserisci un nome per il campo.")

    # Eliminazione campo
    if st.session_state.show_delete_fields:
        st.markdown("### Elimina Campi Esistenti")
        metadata_to_delete = st.multiselect(
            "Seleziona i campi da eliminare definitivamente:",
            options=metadata_fields,
            key="delete_fields_select"
        )

        if st.button("Elimina selezionati"):
            if metadata_to_delete:
                for f in metadata_to_delete:
                    if f in metadata_fields:
                        metadata_fields.remove(f)
                try:
                    with open(METADATA_CONF, "w", encoding="utf-8") as f:
                        json.dump(metadata_fields, f, indent=2, ensure_ascii=False)
                    st.success("Campi eliminati con successo!")
                    st.session_state.show_delete_fields = False
                    st.rerun()
                except Exception as e:
                    st.error(f"Errore durante l'eliminazione: {e}")
            else:
                st.warning("Seleziona almeno un campo da eliminare.")

    # =============================
    # Modifica valori correnti
    # =============================
    st.markdown("---")
    st.subheader("Modifica Metadati del Dataset")

    metadata_fields_current = sorted(set(metadata_fields) | set(st.session_state.metadata_entries.keys()))

    new_field = st.selectbox("Campo Metadata", metadata_fields_current, key="new_field_select")
    new_value = st.text_input(
        "Valore per il campo selezionato",
        value=st.session_state.metadata_entries.get(new_field, ""),
        key="new_value_input"
    )

    if st.button("➕ Aggiungi/Modifica valore"):
        st.session_state.metadata_entries[new_field] = new_value
        metadata_json["metadata"] = st.session_state.metadata_entries
        os.makedirs(os.path.dirname(metadata_file), exist_ok=True)
        st.rerun()

    # =============================
    # Visualizza metadati correnti
    # =============================
    st.markdown("---")
    st.subheader("Metadati Correnti")
    if st.session_state.metadata_entries:
        for key, value in st.session_state.metadata_entries.items():
            col1_disp, col2_disp, col3_disp = st.columns([3, 1, 1])
            with col1_disp:
                st.write(f"**{key}**: {value}")
            with col2_disp:
                if st.button("✏️ Modifica", key=f"edit_btn_{key}"):
                    st.session_state["new_field_select"] = key
                    st.session_state["new_value_input"] = value
                    st.experimental_rerun()
            with col3_disp:
                if st.button("🗑️ Elimina", key=f"delete_btn_{key}"):
                    if key in st.session_state.metadata_entries:
                        del st.session_state.metadata_entries[key]
                        metadata_json["metadata"] = st.session_state.metadata_entries
                        os.makedirs(os.path.dirname(metadata_file), exist_ok=True)
                        st.rerun()
    else:
        st.info("Nessun metadato inserito.")

    # =============================
    # Conferma finale
    # =============================
    st.markdown("---")
    col3, col4 = st.columns([1, 1])
    with col3:
        if st.button("✅ Conferma Metadati"):
            try:
                metadata_json["metadata"] = st.session_state.metadata_entries
                metadata_file = archive_and_update_metadata(st, metadata_file, metadata_json)
                update_master_metadata(metadata_file)
                st.success("Metadati salvati con successo!")
                st.session_state.metadata_confirmed = True
                st.session_state.current_stage = "action_selection"
                st.rerun()
            except Exception as e:
                st.error(f"Errore durante il salvataggio dei metadati: {e}")
