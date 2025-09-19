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
from pipelines.mapping_schema_pipeline import create_pipeline as create_mapping_pipeline

from states.src_schema_state import State
from states.mapping_schema_state import State as MappingState

from nodes.src_schema_nodes.schema_node import SchemaNode
from nodes.src_schema_nodes.human_review_node import HumanReviewNode
from nodes.src_schema_nodes.validation_node import ValidationNode
from nodes.src_schema_nodes.schema_writer_node import SchemaWriter

from nodes.mapping_schema_nodes.mapping_node import MappingNode 
from nodes.mapping_schema_nodes.human_review_node import HumanReviewNode as MappingHumanReviewNode
from nodes.mapping_schema_nodes.validation_node import ValidationNode as MappingValidationNode
from nodes.mapping_schema_nodes.mapping_writer_node import MappingWriter 

from langfuse import Langfuse
from langfuse.langchain import CallbackHandler

import dotenv
dotenv.load_dotenv()

### PATH DOVE SONO SALVATI I DATASETS

BASE_PATH = os.getenv("BASE_PATH", "")
SCHEMA_DIR = os.getenv("SCHEMA_DIR", "")
  

# Configurazione del modello e dei prompt
MODEL_CONFIG = "./config/gemini2.0-flash.yml"
PROMPTS_PATH = "./config/prompts.yml"
METADATA_DIR = "./config/metadata_0.json"

# Inizializzazione di Langfuse
langfuse = Langfuse( 
    public_key= os.environ.get('LANGFUSE_PUBLIC_KEY'),
    secret_key= os.environ.get('LANGFUSE_PRIVATE_KEY'), 
    host= os.environ.get('LANGFUSE_STRING_CONNECTION')
)
langfuse_handler = CallbackHandler()


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


# --- Funzioni di visualizzazione delle sezioni ---

def show_dataset_selection():
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

def show_metadata_editor():
    """Mostra la sezione di gestione e modifica dei metadati."""

    st.subheader("2. Inserisci Metadati del Dataset")
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
        metadata_fields = ["_task", "_size", "_records", "_link", "_dataset_description"]

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

    # Layout a colonne per i pulsanti di estrazione
    col_extraction1, col_extraction2 = st.columns(2)
    with col_extraction1:
        if st.button("Estrai Source Schema dal Dataset con LLM"):
            st.session_state.current_stage = "schema_extraction"
            st.session_state.pipeline_started = False # Reset della pipeline per avvio
            st.session_state.deterministic_extraction = False # Imposta la modalità LLM
            st.rerun()
    
    with col_extraction2:
        if st.button("Estrai Source Schema Deterministicamente"):
            st.session_state.current_stage = "schema_extraction"
            st.session_state.pipeline_started = False # Reset della pipeline per avvio
            st.session_state.deterministic_extraction = True # Imposta la modalità deterministica
            st.rerun()
        
    if st.button("📄 Ho già il Source Schema del dataset"):
        st.session_state.current_stage = "select_target_schema"
        st.rerun()

def show_schema_extraction():
    """Mostra la sezione di estrazione dello schema e gestisce la pipeline."""
    st.subheader("4. Estrazione Automatica dello Schema")
    st.write("La pipeline sta analizzando il tuo dataset per estrarre lo schema. Attendi il completamento o fornisci un feedback.")

    if "pipeline_running" not in st.session_state:
        st.session_state.pipeline_running = False
    
    # Nuovo stato per la modalità di modifica manuale
    if "manual_edit_active" not in st.session_state:
        st.session_state.manual_edit_active = False

    # Avvia la pipeline solo se non è già stata avviata
    if not st.session_state.pipeline_running and not st.session_state.manual_edit_active:
        if "thread_id" not in st.session_state:
            st.session_state.thread_id = str(uuid.uuid4())

        try:
            st.session_state.samples = json.loads(json.dumps(st.session_state.samples))
        except Exception as e:
            st.error(f"Errore durante il parsing del JSON di esempio: {e}")
            return

        # Aggiunta del campo 'deterministic' allo stato iniziale della pipeline
        init_state = State(
            samples=st.session_state.samples,
            output_path=os.path.join(BASE_PATH, st.session_state.selected_folder, "schema.json"),
        )
        
        # Aggiungi 'deterministic=True' se la modalità è stata selezionata
        if st.session_state.get("deterministic_extraction", False):
            st.subheader("4. Estrazione Deterministicamente dello Schema")
            init_state = init_state.copy(update={"deterministic": True})
            st.info("Avvio della pipeline in modalità deterministica.")
        else:
            st.subheader("4. Estrazione Automatica dello Schema (LLM)")
            st.write("La pipeline sta analizzando il tuo dataset per estrarre lo schema. Attendi il completamento o fornisci un feedback.")

        config = {
            "configurable": {"thread_id": st.session_state.thread_id},
            "callbacks": [langfuse_handler]
        }

        try:
            with st.spinner("Avvio della pipeline..."):
                result = st.session_state.src_schema_graph.invoke(init_state, config=config)
            
            st.session_state.interrupt = result.get("__interrupt__")
            st.session_state.state = result
            st.session_state.pipeline_running = True
            st.rerun() 
        except Exception as e:
            st.error(f"Errore durante l'avvio della pipeline: {e}")
            st.error(traceback.format_exc())

    # Sezione per la modifica manuale
    if st.session_state.manual_edit_active:
        st.markdown("---")
        st.subheader("Modifica Manuale dello Schema")
        st.write("Modifica il JSON dello schema qui sotto e premi 'Conferma' per inviare le modifiche alla pipeline per la validazione.")
        
        schema_str_to_edit = ""
        if "interrupt" in st.session_state and st.session_state.interrupt:
            interrupt_val = st.session_state.interrupt
            if isinstance(interrupt_val, list):
                interrupt_val = interrupt_val[0]
            schema_str_to_edit = interrupt_val.value.get("assistant_output", "{}")
        
        edited_schema = st.text_area(
            "Schema JSON da modificare:",
            value=schema_str_to_edit,
            height=400
        )

        feedback_schema = json.dumps(ast.literal_eval(edited_schema))

        print(feedback_schema)
        
        col_edit1, col_edit2 = st.columns(2)
        with col_edit1:
            if st.button("✅ Conferma Modifiche"):
                decision = {"action": "manual", "feedback": feedback_schema}
                config = {"configurable": {"thread_id": st.session_state.thread_id}, "callbacks": [langfuse_handler]}
                
                try:
                    with st.spinner("Invio delle modifiche per la validazione..."):
                        result2 = st.session_state.src_schema_graph.invoke(Command(resume=decision), config=config)
                    
                    if "__interrupt__" in result2:
                        st.session_state.interrupt = result2["__interrupt__"]
                        st.session_state.state = result2
                        st.session_state.manual_edit_active = False # Torna alla vista di review
                        st.rerun()
                    else:
                        st.success("✅ Schema modificato e validato!")
                        st.session_state.current_stage = "select_target_schema"
                        st.session_state.pipeline_running = False
                        st.session_state.manual_edit_active = False
                        st.json(result2)
                        st.balloons()
                        st.rerun()
                except Exception as e:
                    st.error(f"Errore durante l'invio delle modifiche: {e}")
                    st.error(traceback.format_exc())

        with col_edit2:
            if st.button("❌ Annulla Modifica"):
                st.session_state.manual_edit_active = False
                st.rerun()
                
    # Se la pipeline è in esecuzione e ha generato un'interruzione per il feedback dell'utente
    elif "interrupt" in st.session_state and st.session_state.interrupt and not st.session_state.manual_edit_active:
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
        
        # Logica condizionale per visualizzare i pulsanti e il campo di testo
        if st.session_state.get("deterministic_extraction", False):
            # Modalità deterministica: mostra solo i pulsanti necessari
            col1, col2 = st.columns(2)
            with col1:
                if st.button("➡️ Prosegui"):
                    decision = {"action": "break"}
                    config = {"configurable": {"thread_id": st.session_state.thread_id}, "callbacks": [langfuse_handler]}
                    try:
                        with st.spinner("Invio della decisione e ripresa della pipeline..."):
                            result2 = st.session_state.src_schema_graph.invoke(Command(resume=decision), config=config)
                        st.session_state.interrupt = None
                        st.success("✅ Schema approvato e salvato!")
                        st.session_state.current_stage = "select_target_schema"
                        st.session_state.pipeline_running = False
                        st.json(result2)
                        st.balloons()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Errore durante la ripresa della pipeline: {e}")
                        st.error(traceback.format_exc())
            with col2:
                if st.button("✏️ Modifica Manuale"):
                    st.session_state.manual_edit_active = True
                    st.rerun()
        else:
            # Modalità LLM: mostra tutti i pulsanti e il feedback
            feedback_text = st.text_area("Se vuoi ritentare la generazione, scrivi qui il tuo feedback per migliorarla:", value="", key="feedback_input")

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                if st.button("➡️ Prosegui"):
                    decision = {"action": "break"}
                    config = {"configurable": {"thread_id": st.session_state.thread_id}, "callbacks": [langfuse_handler]}
                    try:
                        with st.spinner("Invio della decisione e ripresa della pipeline..."):
                            result2 = st.session_state.src_schema_graph.invoke(Command(resume=decision), config=config)
                        st.session_state.interrupt = None
                        st.success("✅ Schema approvato e salvato!")
                        st.session_state.current_stage = "select_target_schema"
                        st.session_state.pipeline_running = False
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
                            result2 = st.session_state.src_schema_graph.invoke(Command(resume=decision), config=config)
                        if "__interrupt__" in result2:
                            st.session_state.interrupt = result2["__interrupt__"]
                            st.session_state.state = result2
                            st.rerun()
                        else:
                            st.session_state.interrupt = None
                            st.success("✅ Pipeline completata dopo il feedback!")
                            st.session_state.current_stage = "select_target_schema"
                            st.session_state.pipeline_running = False
                            st.json(result2)
                            st.balloons()
                            st.rerun()
                    except Exception as e:
                        st.error(f"Errore durante la ripresa della pipeline: {e}")
                        st.error(traceback.format_exc())
            
            with col3:
                if st.button("↩️ Reset Generazione"):
                    decision = {"action": "restart"}
                    config = {"configurable": {"thread_id": st.session_state.thread_id}, "callbacks": [langfuse_handler]}
                    try:
                        with st.spinner("Richiesta di reset e riavvio della pipeline..."):
                            result2 = st.session_state.src_schema_graph.invoke(Command(resume=decision), config=config)
                        if "__interrupt__" in result2:
                            st.session_state.interrupt = result2["__interrupt__"]
                            st.session_state.state = result2
                            st.rerun()
                        else:
                            st.session_state.interrupt = None
                            st.success("✅ Pipeline riavviata con successo!")
                            st.session_state.current_stage = "select_target_schema"
                            st.session_state.pipeline_running = False
                            st.json(result2)
                            st.balloons()
                            st.rerun()
                    except Exception as e:
                        st.error(f"Errore durante la ripresa della pipeline: {e}")
                        st.error(traceback.format_exc())
            
            with col4:
                if st.button("✏️ Modifica Manuale"):
                    st.session_state.manual_edit_active = True
                    st.rerun()
    
    if st.button("⬅️ Torna a Opzioni Schema", key="back_to_options_from_extraction_btn"):
        st.session_state.current_stage = "schema_extraction_options"
        st.session_state.pipeline_running = False
        st.session_state.manual_edit_active = False
        st.session_state.interrupt = None
        st.rerun()

def show_select_target_schema():
    """Mostra la sezione di selezione del target schema."""
    st.subheader("5. Seleziona Target Schema")
    st.write("Seleziona uno schema JSON esistente da usare come target per la trasformazione.")
    
    if not SCHEMA_DIR or not os.path.isdir(SCHEMA_DIR):
        st.error("SCHEMA_DIR non configurato correttamente o la directory non esiste.")
        return
    
    schema_files = [f for f in os.listdir(SCHEMA_DIR) if f.endswith('.json') and os.path.isfile(os.path.join(SCHEMA_DIR, f))]
    
    if not schema_files:
        st.warning("Nessun file schema JSON trovato nella directory specificata.")
        if st.button("⬅️ Torna a Opzioni Schema", key="back_to_options_from_target_btn"):
            st.session_state.current_stage = "schema_extraction_options"
            st.rerun()
        return

    # Visualizza i metadati
    st.subheader("Metadati del Dataset")
    st.write("Questi sono i metadati associati al dataset selezionato.")
    
    dataset_path = os.path.join(BASE_PATH, st.session_state.selected_folder)
    metadata_path = os.path.join(dataset_path, "metadata_0.json")
    if os.path.exists(metadata_path):
        try:
            with open(metadata_path, "r", encoding="utf-8") as f:
                metadata_content = json.load(f)
                st.json(metadata_content)
        except Exception as e:
            st.error(f"Errore nella lettura dei metadati: {e}")
    else:
        st.warning("File dei metadati (metadata_0.json) non trovato per questo dataset.")

    st.markdown("---")

    # Selettore per il file schema
    st.session_state.selected_target_schema_file = st.selectbox(
        "Seleziona un file schema:",
        options=[""] + schema_files,
        index=0,
        key="target_schema_file_select"
    )

    if st.session_state.selected_target_schema_file:
        selected_file_path = os.path.join(SCHEMA_DIR, st.session_state.selected_target_schema_file)
        
        st.markdown("---")
        st.subheader("Anteprima dello Schema Selezionato")
        try:
            with open(selected_file_path, "r", encoding="utf-8") as f:
                schema_content = json.load(f)
                st.json(schema_content)
        except Exception as e:
            st.error(f"Errore nella lettura del file schema: {e}")
            st.warning("Assicurati che il file sia un JSON valido.")
            return

        st.markdown("---")
        
        st.session_state.target_schema_comment = st.text_area(
            "Aggiungi un commento per l'LLM (opzionale):",
            "Fornisci istruzioni o contesto aggiuntivo per aiutare l'LLM a mappare i campi.",
            key="target_schema_comment_box"
        )
        
        st.success("Target schema e commento pronti.")
    
    st.markdown("---")
    
    st.subheader("Avvia Pipeline di creazione mapping tra src schema+metadati e target schema")
    st.write("Qui è dove potrai avviare la pipeline per generare il mapping tra lo schema sorgente + metadati e il target schema selezionato.")

    if st.button("⬅️ Torna a Opzioni Schema", key="back_to_options_from_target_btn"):
        st.session_state.current_stage = "schema_extraction_options"
        st.session_state.pipeline_started = False
        st.rerun()

    # Avvia la pipeline di mapping
    if st.session_state.get("selected_target_schema_file"):
        if st.button("🚀 Avvia Creazione Mapping"):
            st.session_state.current_stage = "mapping_generation"
            st.session_state.pipeline_started = False
            st.session_state.manual_edit_active = False
            st.rerun()

def show_mapping_generation():
    """Gestisce la visualizzazione e l'esecuzione della pipeline di mapping."""
    st.subheader("6. Generazione del Mapping")
    st.write("La pipeline sta generando il mapping tra lo schema sorgente e quello di destinazione.")

    if "pipeline_running" not in st.session_state:
        st.session_state.pipeline_running = False
    
    if "manual_edit_active" not in st.session_state:
        st.session_state.manual_edit_active = False

    if not st.session_state.pipeline_running and not st.session_state.manual_edit_active:
        if "thread_id_mapping" not in st.session_state:
            st.session_state.thread_id_mapping = str(uuid.uuid4())
        
        # Carica il src_schema salvato dal passo precedente
        src_schema_path = os.path.join(BASE_PATH, st.session_state.selected_folder, "schema.json")
        src_metadata_path = os.path.join(BASE_PATH, st.session_state.selected_folder, "metadata_0.json")

        try:
            with open(src_schema_path, 'r', encoding='utf-8') as f:
                st.session_state.src_schema = json.load(f)

            with open(src_metadata_path, 'r', encoding='utf-8') as f:
                st.session_state.src_metadata = json.load(f)
        except Exception as e:
            st.error(f"Errore nel caricamento dello schema sorgente: {e}")
            return
        
        mapping_state = MappingState(
            samples=st.session_state.samples,
            src_schema=st.session_state.src_schema,
            dst_schema=st.session_state.dst_schema,
            metadata=st.session_state.src_metadata,
            output_path=os.path.join(BASE_PATH, st.session_state.selected_folder, "mapping.json"),
        )
        
        config = {
            "configurable": {"thread_id": st.session_state.thread_id_mapping},
            "callbacks": [langfuse_handler]
        }
        
        try:
            with st.spinner("Avvio della pipeline di mapping..."):
                result = st.session_state.mapping_graph.invoke(mapping_state, config=config)
            
            st.session_state.interrupt = result.get("__interrupt__")
            st.session_state.state = result
            st.session_state.pipeline_running = True
            st.rerun()
        except Exception as e:
            st.error(f"Errore durante l'avvio della pipeline di mapping: {e}")
            st.error(traceback.format_exc())
        # Logica per visualizzare il mapping e gestire il feedback dell'utente
    elif "interrupt" in st.session_state and st.session_state.interrupt and not st.session_state.manual_edit_active:
        interrupt = st.session_state.interrupt
        if isinstance(interrupt, list):
            interrupt = interrupt[0]

        st.markdown("---")
        st.subheader("Esame del Mapping Generato")
        st.write("Rivedi il mapping generato e decidi se approvarlo, modificarlo o fornire un feedback.")
        
        mapping_str = interrupt.value.get("assistant_output", "{}")
        if mapping_str and (mapping_str.strip().startswith('{') or mapping_str.strip().startswith('[')):
            try:
                mapping_dict = json.loads(mapping_str)
                st.json(mapping_dict)
            except Exception:
                try:
                    mapping_dict = ast.literal_eval(mapping_str)
                    st.json(mapping_dict)
                except Exception:
                    st.warning("Impossibile parsare il mapping come JSON/dizionario. Visualizzo come testo.")
                    st.write(mapping_str)
        else:
            st.write(mapping_str)

        st.markdown("---")
        
        feedback_text = st.text_area(
            "Se vuoi ritentare la generazione, scrivi qui il tuo feedback per migliorarla:", 
            value="", 
            key="mapping_feedback_input"
        )
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("➡️ Prosegui"):
                decision = {"action": "break"}
                config = {"configurable": {"thread_id": st.session_state.thread_id_mapping}, "callbacks": [langfuse_handler]}
                try:
                    with st.spinner("Invio della decisione e ripresa della pipeline..."):
                        result = st.session_state.mapping_graph.invoke(Command(resume=decision), config=config)
                    st.session_state.interrupt = None
                    st.session_state.pipeline_running = False
                    st.success("✅ Mapping approvato e salvato!")
                    st.session_state.current_stage = "end" # Nuovo stadio finale
                    st.json(result)
                    st.balloons()
                    st.rerun()
                except Exception as e:
                    st.error(f"Errore durante la ripresa della pipeline: {e}")
                    st.error(traceback.format_exc())

        with col2:
            if st.button("🔄 Ritenta Generazione"):
                decision = {"action": "continue", "feedback": feedback_text}
                config = {"configurable": {"thread_id": st.session_state.thread_id_mapping}, "callbacks": [langfuse_handler]}
                try:
                    with st.spinner("Invio della decisione e ripresa della pipeline..."):
                        result = st.session_state.mapping_graph.invoke(Command(resume=decision), config=config)
                    if "__interrupt__" in result:
                        st.session_state.interrupt = result["__interrupt__"]
                        st.session_state.state = result
                        st.rerun()
                    else:
                        st.session_state.interrupt = None
                        st.session_state.pipeline_running = False
                        st.success("✅ Pipeline completata dopo il feedback!")
                        st.session_state.current_stage = "end"
                        st.json(result)
                        st.balloons()
                        st.rerun()
                except Exception as e:
                    st.error(f"Errore durante la ripresa della pipeline: {e}")
                    st.error(traceback.format_exc())
        
        with col3:
            if st.button("↩️ Reset Generazione"):
                decision = {"action": "restart"}
                config = {"configurable": {"thread_id": st.session_state.thread_id_mapping}, "callbacks": [langfuse_handler]}
                try:
                    with st.spinner("Richiesta di reset e riavvio della pipeline..."):
                        result = st.session_state.mapping_graph.invoke(Command(resume=decision), config=config)
                    if "__interrupt__" in result:
                        st.session_state.interrupt = result["__interrupt__"]
                        st.session_state.state = result
                        st.rerun()
                    else:
                        st.session_state.interrupt = None
                        st.session_state.pipeline_running = False
                        st.success("✅ Pipeline riavviata con successo!")
                        st.session_state.current_stage = "end"
                        st.json(result)
                        st.balloons()
                        st.rerun()
                except Exception as e:
                    st.error(f"Errore durante la ripresa della pipeline: {e}")
                    st.error(traceback.format_exc())
        
        with col4:
            if st.button("✏️ Modifica Manuale"):
                st.session_state.manual_edit_active = True
                st.rerun()

    # Sezione per la modifica manuale
    if st.session_state.manual_edit_active:
        st.markdown("---")
        st.subheader("Modifica Manuale del Mapping")
        st.write("Modifica il JSON del mapping qui sotto e premi 'Conferma' per inviare le modifiche alla pipeline per la validazione.")
        
        mapping_str_to_edit = ""
        if "interrupt" in st.session_state and st.session_state.interrupt:
            interrupt_val = st.session_state.interrupt
            if isinstance(interrupt_val, list):
                interrupt_val = interrupt_val[0]
            mapping_str_to_edit = interrupt_val.value.get("assistant_output", "{}")
        
        edited_mapping = st.text_area(
            "Mapping JSON da modificare:",
            value=mapping_str_to_edit,
            height=400
        )
        feedback_mapping = json.dumps(ast.literal_eval(edited_mapping))

        col_edit1, col_edit2 = st.columns(2)
        with col_edit1:
            if st.button("✅ Conferma Modifiche"):
                decision = {"action": "manual", "feedback": feedback_mapping}
                config = {"configurable": {"thread_id": st.session_state.thread_id_mapping}, "callbacks": [langfuse_handler]}
                
                try:
                    with st.spinner("Invio delle modifiche per la validazione..."):
                        result = st.session_state.mapping_graph.invoke(Command(resume=decision), config=config)
                    
                    if "__interrupt__" in result:
                        st.session_state.interrupt = result["__interrupt__"]
                        st.session_state.state = result
                        st.session_state.manual_edit_active = False
                        st.rerun()
                    else:
                        st.success("✅ Mapping modificato e validato!")
                        st.session_state.current_stage = "end"
                        st.session_state.pipeline_running = False
                        st.session_state.manual_edit_active = False
                        st.json(result)
                        st.balloons()
                        st.rerun()
                except Exception as e:
                    st.error(f"Errore durante l'invio delle modifiche: {e}")
                    st.error(traceback.format_exc())

        with col_edit2:
            if st.button("❌ Annulla Modifica"):
                st.session_state.manual_edit_active = False
                st.rerun()

# --- Funzione principale per la gestione degli stati ---

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
        show_dataset_selection()
    
    elif st.session_state.current_stage in ["metadata", "confirm_delete"]:
        # Assicurati che il percorso del dataset sia sempre aggiornato
        if st.session_state.selected_folder and st.session_state.selected_subfolder:
            st.session_state.dataset_path = os.path.join(BASE_PATH, st.session_state.selected_folder)
            st.session_state.dataset_data = os.path.join(st.session_state.dataset_path, st.session_state.selected_subfolder)
            st.session_state.samples = load_dataset_samples(st.session_state.dataset_data, k=1)
        show_metadata_editor()
    
    elif st.session_state.current_stage == "schema_extraction_options":
        show_schema_options()
    
    elif st.session_state.current_stage == "schema_extraction":
        show_schema_extraction()
        
    elif st.session_state.current_stage == "select_target_schema":
        show_select_target_schema()
        
    elif st.session_state.current_stage == "mapping_generation":
        show_mapping_generation()
        
    elif st.session_state.current_stage == "end":
        st.subheader("Processo Completato! 🎉")
        st.write("Le pipeline di estrazione schema e di generazione mapping sono state completate con successo. Ora puoi trovare i file `schema.json` e `mapping.json` nella tua cartella del dataset.")
        if st.button("Torna all'inizio"):
            st.session_state.clear()
            st.rerun()

if __name__ == "__main__":
    main()