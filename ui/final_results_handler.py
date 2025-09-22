
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
    else:
        st.warning("Nessun risultato di mapping trovato. Per favore, esegui prima la pipeline.")
        if st.button("Torna alla Mappatura"):
            st.session_state.current_stage = "mapping_generation"
            st.rerun()