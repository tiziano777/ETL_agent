import streamlit as st
import json
import ast
import uuid
import os
import traceback
import glob

from langgraph.types import Command
from states.mapping_schema_state import State as MappingState

import dotenv
dotenv.load_dotenv()
METADATA_PATH = os.getenv("METADATA_PATH", "")
SCHEMA_DIR = os.getenv("SCHEMA_DIR", "")

def show_select_target_schema(st):
    """Mostra la sezione di selezione del target schema (STEP 5)."""
    st.subheader("Seleziona Schema di Destinazione")
    st.write("Seleziona uno schema JSON esistente da usare come destinazione per la trasformazione.")
    
    if not SCHEMA_DIR or not os.path.isdir(SCHEMA_DIR):
        st.error("La variabile d'ambiente SCHEMA_DIR non è configurata o la directory non esiste.")
        return
    
    schema_files = [f for f in os.listdir(SCHEMA_DIR) if f.endswith('.json') and os.path.isfile(os.path.join(SCHEMA_DIR, f))]
    
    if not schema_files:
        st.warning("Nessun file schema JSON trovato nella directory specificata.")
        if st.button("⬅️ Torna Indietro", key="back_to_prev_stage_btn"):
            # Assumendo che lo step precedente sia 'schema_extraction_options' o simile
            st.session_state.current_stage = "action_selection"
            st.rerun()
        return

    # Selettore per il file schema
    st.session_state.selected_target_schema_file = st.selectbox(
        "Seleziona un file schema di destinazione:",
        options=[""] + schema_files,
        key="target_schema_file_select"
    )

    if st.session_state.selected_target_schema_file:
        selected_file_path = os.path.join(SCHEMA_DIR, st.session_state.selected_target_schema_file)
        
        st.markdown("---")
        st.subheader("Anteprima dello Schema di Destinazione Selezionato")
        try:
            with open(selected_file_path, "r", encoding="utf-8") as f:
                st.session_state.dst_schema = json.load(f)
                st.json(st.session_state.dst_schema)
        except Exception as e:
            st.error(f"Errore nella lettura del file schema: {e}")
            return
        
        st.session_state.target_schema_comment = st.text_area(
            "Aggiungi un commento o istruzioni per l'LLM (opzionale):",
            key="target_schema_comment_box"
        )
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("⬅️ Torna Indietro", key="back_to_options_from_target_btn", use_container_width=True):
            st.session_state.current_stage = "action_selection" # O lo stage corretto
            st.rerun()

    with col2:
        if st.session_state.get("selected_target_schema_file"):
            if st.button("🚀 Avvia Creazione Mapping", type="primary", use_container_width=True):
                st.session_state.current_stage = "mapping_generation"
                # Resetta stati della pipeline precedente per sicurezza
                st.session_state.pipeline_running = False
                st.session_state.manual_edit_active = False
                st.rerun()

def show_mapping_generation(st, langfuse_handler):
    """Gestisce la visualizzazione e l'esecuzione della pipeline di mapping (STEP 6)."""
    st.subheader("Generazione e Validazione del Mapping")

    # Inizializzazione degli stati di sessione specifici per il mapping
    st.session_state.setdefault("pipeline_running", False)
    st.session_state.setdefault("manual_edit_active", False)
    st.session_state.setdefault("interrupt", None)

    # ===================================================================
    # FUNZIONE HELPER PER GESTIRE LA RIPRESA DELLA PIPELINE DI MAPPING
    # ===================================================================
    def resume_mapping_pipeline(decision: dict):
        """Invia una decisione alla pipeline di mapping e gestisce la sua risposta."""
        config = {"configurable": {"thread_id": st.session_state.thread_id_mapping}, "callbacks": [langfuse_handler]}
        try:
            with st.spinner("La pipeline di mapping sta elaborando..."):
                result = st.session_state.mapping_graph.invoke(Command(resume=decision), config=config)
            
            # Controlla se la pipeline si è interrotta di nuovo
            if "__interrupt__" in result:
                print("La pipeline richiede un'altra revisione. Questo può accadere dopo un fallimento di validazione.")
                st.session_state.interrupt = result["__interrupt__"]
                st.session_state.state = result
                st.session_state.manual_edit_active = False
            else:
                st.success("✅ Mapping approvato e salvato con successo!")
                st.balloons()
                st.session_state.interrupt = None
                st.session_state.pipeline_running = False
                st.session_state.manual_edit_active = False
                st.session_state.state = result # Salva lo stato di output
                st.session_state.current_stage = "mapping_results" # Vai al nuovo stadio di risultati
        
        except Exception as e:
            print(traceback.format_exc())
            print(f"Errore durante la ripresa della pipeline di mapping: {e}")
            st.error(f"Errore durante la ripresa della pipeline di mapping: {e}")
            st.error(traceback.format_exc())
        
        st.rerun()
        
    # ===================================================================
    # 1. AVVIO INIZIALE DELLA PIPELINE DI MAPPING
    # ===================================================================
    if not st.session_state.pipeline_running and not st.session_state.manual_edit_active:
        if "thread_id_mapping" not in st.session_state:
            st.session_state.thread_id_mapping = str(uuid.uuid4())
        
        try:
            metadata_path = max(glob.glob(os.path.join(METADATA_PATH,f"{st.session_state.selected_version}__{st.session_state.selected_dataset_name}__{st.session_state.selected_subpath}__*.json")))
            with open(metadata_path, 'r', encoding='utf-8') as f:
                metadata_content = json.load(f)
            src_schema= metadata_content.get("src_schema")
            metadata= metadata_content.get("metadata")
                        
        except Exception as e:
            st.error(f"Errore nel caricamento dei file di schema sorgente o metadati: {e}")
            if st.button("⬅️ Torna alla Selezione Generazione schema sorgente", key="back_to_src_schema_btn"):
                st.session_state.current_stage = "schema_extraction_options"
                st.rerun()
        
        mapping_state = MappingState(
            samples=st.session_state.samples,
            src_schema=src_schema,
            dst_schema=st.session_state.dst_schema,
            metadata=metadata,
            output_path=max(glob.glob(os.path.join(METADATA_PATH,f"{st.session_state.selected_version}__{st.session_state.selected_dataset_name}__{st.session_state.selected_subpath}__*.json"))),
        )
        
        config = {"configurable": {"thread_id": st.session_state.thread_id_mapping}, "callbacks": [langfuse_handler]}
        
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

    # ===================================================================
    # 2. INTERFACCIA DI MODIFICA MANUALE
    # ===================================================================
    elif st.session_state.manual_edit_active:
        st.markdown("---")
        st.subheader("Modifica Manuale del Mapping")
        
        interrupt_val = st.session_state.interrupt
        if isinstance(interrupt_val, list): interrupt_val = interrupt_val[0]
        mapping_str_to_edit = interrupt_val.value.get("assistant_output", "{}")
        
        edited_mapping_str = st.text_area("Mapping JSON:", value=mapping_str_to_edit, height=400)

        col1, col2 = st.columns(2)
        if col1.button("✅ Conferma Modifiche", use_container_width=True):
            try:
                feedback_mapping = json.dumps(json.loads(edited_mapping_str))
                resume_mapping_pipeline({"action": "manual", "feedback": feedback_mapping})
            except json.JSONDecodeError:
                st.error("Errore: il testo inserito non è un JSON valido.")

        if col2.button("❌ Annulla Modifica", use_container_width=True):
            st.session_state.manual_edit_active = False
            st.rerun()

    # ===================================================================
    # 3. INTERFACCIA DI REVIEW (FEEDBACK UMANO)
    # ===================================================================
    elif st.session_state.interrupt:
        interrupt = st.session_state.interrupt
        if isinstance(interrupt, list): interrupt = interrupt[0]

        st.markdown("---")
        st.subheader("Esame del Mapping Generato")
        
        mapping_str = interrupt.value.get("assistant_output", "{}")
        try:
            st.json(ast.literal_eval(mapping_str))
        except (ValueError, SyntaxError):
            st.code(mapping_str, language='json')
        
        validation_error = st.session_state.state.get("validation_error")
        if validation_error:
            st.error(f"⚠️ **Errore di Validazione Precedente:**\n\n{validation_error}")
            st.warning("Il mapping è stato rigenerato. Per favore, rivedilo.")

        st.markdown("---")
        
        feedback_text = st.text_area("Feedback per migliorare la generazione:", key="mapping_feedback_input")
        
        cols = st.columns(4)
        if cols[0].button("➡️ Prosegui alla Validazione", use_container_width=True):
            resume_mapping_pipeline({"action": "break"})
        if cols[1].button("🔄 Ritenta con Feedback", use_container_width=True):
            resume_mapping_pipeline({"action": "continue", "feedback": feedback_text})
        if cols[2].button("↩️ Ricomincia da Capo", use_container_width=True):
            resume_mapping_pipeline({"action": "restart"})
        if cols[3].button("✏️ Modifica Manuale", use_container_width=True):
            st.session_state.manual_edit_active = True
            st.rerun()

    # ===================================================================
    # Pulsante per tornare indietro
    # ===================================================================
    if st.button("⬅️ Torna alla Selezione Schema Target"):
        st.session_state.current_stage = "select_target_schema"
        # Resetta tutti gli stati specifici di questa pipeline
        for key in ["pipeline_running", "manual_edit_active", "interrupt", "state", "thread_id_mapping"]:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()

def show_mapping_results(st):
    """Visualizza i risultati finali del mapping."""
    st.subheader("🎉 Risultati del Mapping")
    st.write("Confronta i campioni grezzi a sinistra con i dati mappati a destra per validare il risultato.")

    final_state = st.session_state.get("state")
    if final_state and hasattr(final_state, 'get'):
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Samples Originali**")
            st.json(final_state.get("samples"))

        with col2:
            st.markdown("**Risultato del Mapping**")
            st.json(final_state.get("mapped_samples"))
        
        st.markdown("---")
        
        if st.button("⬅️ Torna alla Generazione Mapping"):
            st.session_state.current_stage = "mapping_generation"
            st.session_state.pipeline_running = False
            st.session_state.manual_edit_active = False
            st.session_state.interrupt = None
            st.rerun()
        if st.button("Torna alla Home"):
            st.session_state.current_stage = "action_selection"
            st.rerun()

    else:
        st.warning("Nessun risultato di mapping trovato. Per favore, esegui prima la pipeline.")
        if st.button("Torna alla Mappatura"):
            st.session_state.current_stage = "mapping_generation"
            st.rerun()
        if st.button("Torna alla Home"):
            st.session_state.current_stage = "action_selection"
            st.rerun()
        