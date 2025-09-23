import os
import json

import dotenv
dotenv.load_dotenv()
BASE_PATH = os.getenv("BASE_PATH", "")

METADATA_DIR = "./config/metadata_0.json"

def show_dataset_selection(st):
    """Mostra la sezione di selezione del dataset e del sottocartella."""
    st.subheader("1. Seleziona Dataset e Sottocartella")
    if not BASE_PATH or not os.path.isdir(BASE_PATH):
        st.error("BASE_PATH non configurato correttamente nel file `.env`.")
        return

    # Sezione per la selezione del dataset principale
    folders = [f for f in os.listdir(BASE_PATH) if os.path.isdir(os.path.join(BASE_PATH, f))]
    search_query = st.text_input("Cerca dataset/cartella", key="dataset_search")
    filtered_folders = [f for f in folders if search_query.lower() in f.lower()]
    
    selected_main_folder = st.selectbox(
        "Seleziona una cartella del dataset",
        options=[""] + filtered_folders,
        index=0,
        key="selected_folder_box"
    )

    # Aggiorna lo stato della sessione solo se la selezione cambia
    if selected_main_folder != st.session_state.get("selected_folder", ""):
        st.session_state.selected_folder = selected_main_folder
        # Pulisce la selezione del sottocartella quando la cartella principale cambia
        st.session_state.selected_subfolder = ""
        st.rerun()

    # Se un dataset è stato selezionato, mostra il selettore per le sottocartelle
    if st.session_state.selected_folder:
        dataset_path = os.path.join(BASE_PATH, st.session_state.selected_folder)
        subfolders = [d for d in os.listdir(dataset_path) if os.path.isdir(os.path.join(dataset_path, d))]
        
        # Sezione per la selezione del sottocartella
        st.session_state.selected_subfolder = st.selectbox(
            "Seleziona la sottocartella con i dati",
            options=[""] + subfolders,
            index=0,
            key="selected_subfolder_box"
        )
        
        # Pulsante di conferma per procedere
        if st.session_state.selected_subfolder:
            if st.button("✅ Conferma Selezione"):
                st.success(f"Hai selezionato il dataset: {st.session_state.selected_folder}, sottocartella: {st.session_state.selected_subfolder}")
                st.session_state.current_stage = "metadata"
                st.rerun()

def show_metadata_editor(st):
    """Mostra la sezione di gestione e modifica dei metadati."""

    st.subheader("Inserisci Metadati del Dataset")
    st.write("Inserisci e modifica i metadati chiave per il tuo dataset.")

    # Pulsante per tornare alla selezione
    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("⬅️ Torna alla selezione del Dataset"):
            st.session_state.current_stage = "dataset_selection"
            st.session_state.metadata_confirmed = False
            st.rerun()

    # Path
    dataset_path = os.path.join(BASE_PATH, st.session_state.selected_folder)
    metadata_file = os.path.join(dataset_path, "metadata_0.json")

    # =============================
    # Carica i campi disponibili (schema)
    # =============================
    try:
        with open(METADATA_DIR, "r", encoding="utf-8") as f:
            metadata_fields = json.load(f)
    except FileNotFoundError:
        metadata_fields = [ "_size", "_records", "_link", "_dataset_description"]

    # =============================
    # Carica i valori correnti dal file del dataset
    # =============================
    metadata_dict = {}
    if os.path.exists(metadata_file):
        try:
            with open(metadata_file, "r", encoding="utf-8") as f:
                metadata_dict = json.load(f)
        except Exception:
            st.warning("Impossibile caricare i metadati del dataset, inizializzo con vuoto.")

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
                        with open(METADATA_DIR, "w", encoding="utf-8") as f:
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
                    with open(METADATA_DIR, "w", encoding="utf-8") as f:
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
        # salvataggio immediato
        with open(metadata_file, "w", encoding="utf-8") as f:
            json.dump(st.session_state.metadata_entries, f, indent=2, ensure_ascii=False)
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
                        with open(metadata_file, "w", encoding="utf-8") as f:
                            json.dump(st.session_state.metadata_entries, f, indent=2, ensure_ascii=False)
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
                with open(metadata_file, "w", encoding="utf-8") as f:
                    json.dump(st.session_state.metadata_entries, f, indent=2, ensure_ascii=False)
                st.success("Metadati salvati con successo!")
                st.session_state.metadata_confirmed = True
                st.session_state.current_stage = "action_selection"
                st.rerun()
            except Exception as e:
                st.error(f"Errore durante il salvataggio dei metadati: {e}")
