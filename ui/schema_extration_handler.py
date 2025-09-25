import json
import ast
import uuid
import os
import traceback
import glob
from langgraph.types import Command
from states.src_schema_state import State


import dotenv
dotenv.load_dotenv()
METADATA_PATH = os.getenv("METADATA_PATH")

def show_schema_options(st):
    """Mostra la sezione di estrazione o importazione dello schema."""
    st.subheader("Estrazione dello Schema")
    st.write("Vuoi estrarre lo schema dal dataset o ne hai già uno?")
    
    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("⬅️ Torna alla Home"):
            st.session_state.current_stage = "action_selection"
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
        st.session_state.current_stage = "action_selection"
        st.rerun()

def show_schema_extraction(st, langfuse_handler):
    """
    Mostra la sezione di estrazione dello schema e gestisce la pipeline
    con cicli di feedback e validazione.
    """
    st.subheader("Estrazione dello Schema")

    # Avvia la pipeline solo se l'utente ha già scelto la tecnica di estrazione
    if "deterministic_extraction" not in st.session_state:
        st.info("Seleziona prima la tecnica di estrazione schema nella schermata precedente.")
        return

    # Inizializzazione degli stati di sessione
    st.session_state.setdefault("pipeline_running", False)
    st.session_state.setdefault("manual_edit_active", False)
    st.session_state.setdefault("interrupt", None)
    st.session_state.setdefault("validation_success", False) # Nuovo stato per la validazione

    # ===================================================================
    # FUNZIONE HELPER PER GESTIRE LA RIPRESA DELLA PIPELINE
    # ===================================================================
    def resume_pipeline(decision: dict):
        """
        Invia una decisione alla pipeline e gestisce la sua risposta.
        Aggiorna lo stato di Streamlit in base al risultato (nuova interruzione o fine).
        """
        config = {"configurable": {"thread_id": st.session_state.thread_id}, "callbacks": [langfuse_handler]}
        try:
            with st.spinner("La pipeline sta elaborando la tua richiesta..."):
                result = st.session_state.src_schema_graph.invoke(Command(resume=decision), config=config)
            
            # Controlla se la pipeline si è interrotta di nuovo
            if "__interrupt__" in result:
                st.info("La pipeline richiede un ulteriore feedback.")
                st.session_state.interrupt = result["__interrupt__"]
                st.session_state.state = result
                st.session_state.manual_edit_active = False # Assicura di tornare alla vista di review
                st.session_state.validation_success = False # Resetta lo stato di successo
            else:
                # La pipeline è terminata con successo
                st.success("✅ Pipeline completata con successo!")
                st.session_state.interrupt = None
                st.session_state.pipeline_running = False
                st.session_state.manual_edit_active = False
                st.session_state.state = result # Aggiorna lo stato finale
                # Controlla se lo schema è valido per mostrare la schermata di successo
                if st.session_state.state.get("valid"):
                    st.session_state.validation_success = True
                else:
                    st.session_state.validation_success = False

        except Exception as e:
            st.error(f"Errore durante la ripresa della pipeline: {e}")
            st.error(traceback.format_exc())
            st.session_state.validation_success = False
            
        st.rerun() # Aggiorna sempre l'interfaccia dopo un'azione

    # ===================================================================
    # 1. AVVIO INIZIALE DELLA PIPELINE
    # ===================================================================
    if not st.session_state.pipeline_running and not st.session_state.manual_edit_active and not st.session_state.validation_success:
        if "thread_id" not in st.session_state:
            st.session_state.thread_id = str(uuid.uuid4())

        try:
            samples = json.loads(json.dumps(st.session_state.samples))
        except Exception as e:
            st.error(f"Errore nel parsing dei campioni JSON: {e}")
            return

        print("Samples caricati per l'estrazione schema:", samples)

        init_state = State(
            samples=samples,
            output_path=max(glob.glob(os.path.join(METADATA_PATH, f"{st.session_state.selected_version}__{st.session_state.selected_dataset_name}__{st.session_state.selected_subpath}__*.json"))),
        )
        
        if st.session_state.get("deterministic_extraction", False):
            st.subheader("Estrazione Deterministicamente dello Schema")
            init_state = init_state.copy(update={"deterministic": True})
        else:
            st.subheader("Estrazione Automatica dello Schema (LLM)")
        
        st.write("La pipeline sta analizzando il tuo dataset per estrarre lo schema...")
        
        config = {"configurable": {"thread_id": st.session_state.thread_id}, "callbacks": [langfuse_handler]}

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

    # ===================================================================
    # 2. INTERFACCIA DI SUCCESSO E REINDIRIZZAMENTO
    # ===================================================================
    elif st.session_state.validation_success:
        st.success("✅ Schema validato correttamente!")
        st.subheader("Schema visualizzato a schermo")
        # Visualizza lo schema
        st.json(st.session_state.state.get("generated_schema", "{}"))

        st.markdown("---")
        
        col1, col2 = st.columns(2)
        if col1.button("✅ OK"):
            # Resetta gli stati e torna alla home
            for key in ["pipeline_running", "manual_edit_active", "interrupt", "state", "thread_id", "validation_success"]:
                if key in st.session_state:
                    del st.session_state[key]
            st.session_state.current_stage = "action_selection"
            st.rerun()
        if col2.button("⬅️ Torna Indietro"):
            # Resetta lo stato di successo e torna alla schermata di estrazione per ritentare
            st.session_state.validation_success = False
            st.rerun()

    # ===================================================================
    # 3. INTERFACCIA DI MODIFICA MANUALE
    # ===================================================================
    elif st.session_state.manual_edit_active:
        st.markdown("---")
        st.subheader("Modifica Manuale dello Schema")
        st.write("Modifica lo schema JSON e conferma per inviarlo alla validazione.")
        
        interrupt_val = st.session_state.interrupt
        if isinstance(interrupt_val, list): interrupt_val = interrupt_val[0]
        schema_str_to_edit = interrupt_val.value.get("assistant_output", "{}")
        
        edited_schema_str = st.text_area("Schema JSON:", value=schema_str_to_edit, height=400)

        col1, col2 = st.columns(2)
        if col1.button("✅ Conferma Modifiche", use_container_width=True):
            try:
                # Validazione e formattazione del JSON prima dell'invio
                feedback_schema = json.dumps(json.loads(edited_schema_str))
                decision = {"action": "manual", "feedback": feedback_schema}
                resume_pipeline(decision)
            except json.JSONDecodeError:
                st.error("Errore: il testo inserito non è un JSON valido.")
            except Exception as e:
                st.error(f"Errore imprevisto: {e}")

        if col2.button("❌ Annulla Modifica", use_container_width=True):
            st.session_state.manual_edit_active = False
            st.rerun()

    # ===================================================================
    # 4. INTERFACCIA DI REVIEW (FEEDBACK UMANO)
    # ===================================================================
    elif st.session_state.interrupt:
        interrupt = st.session_state.interrupt
        if isinstance(interrupt, list): interrupt = interrupt[0]

        st.markdown("---")
        st.subheader("Campioni del Dataset Utilizzati")
        st.json(st.session_state.samples)

        st.markdown("---")
        st.subheader("Esame dello Schema Generato")
        schema_str = interrupt.value.get("assistant_output", "{}")
        try:
            st.json(ast.literal_eval(schema_str))
        except (ValueError, SyntaxError):
            st.code(schema_str, language='json')
        
        # Se c'è un errore di validazione, mostralo all'utente
        validation_error = st.session_state.state.get("validation_error")
        if validation_error:
            st.error(f"⚠️ **Errore di Validazione Precedente:**\n\n{validation_error}")
            st.warning("Lo schema è stato rigenerato. Per favore, rivedilo e fornisci un nuovo feedback.")


        st.markdown("---")
        
        # --- Azioni per la modalità LLM ---
        if not st.session_state.get("deterministic_extraction", False):
            feedback_text = st.text_area("Se vuoi ritentare la generazione, fornisci un feedback per migliorarla:", key="feedback_input")
            cols = st.columns(4)
            if cols[0].button("➡️ Prosegui alla Validazione", use_container_width=True):
                resume_pipeline({"action": "break"})
            if cols[1].button("🔄 Ritenta con Feedback", use_container_width=True):
                resume_pipeline({"action": "continue", "feedback": feedback_text})
            if cols[2].button("↩️ Ricomincia da Capo", use_container_width=True):
                    resume_pipeline({"action": "restart"})
            if cols[3].button("✏️ Modifica Manuale", use_container_width=True):
                st.session_state.manual_edit_active = True
                st.rerun()
        
        # --- Azioni per la modalità Deterministica ---
        else:
            cols = st.columns(2)
            if cols[0].button("➡️ Prosegui alla Validazione", use_container_width=True):
                resume_pipeline({"action": "break"})
            if cols[1].button("✏️ Modifica Manuale", use_container_width=True):
                st.session_state.manual_edit_active = True
                st.rerun()