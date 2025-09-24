
def show_action_selection(st_app):
    st_app.header("Scegli l'Operazione")
    st_app.write("Quale operazione desideri eseguire sul tuo dataset?")
    
    col1, col2, col3 = st_app.columns(3)
    with col1:
        # Bottone per lanciare la pipeline di estrazione dello schema
        if st_app.button("Estrai Schema", use_container_width=True):
            st_app.session_state.current_stage = "schema_extraction_options"
            st_app.rerun()
    with col2:
        # Bottone per lanciare la pipeline di generazione del mapping
        if st_app.button("Genera Mapping", use_container_width=True):
            st_app.session_state.current_stage = "select_target_schema"
            st_app.rerun()
    with col3:
        # Bottone per tornare a modificare i metadati
        if st_app.button("Modifica Metadati", use_container_width=True):
            st_app.session_state.current_stage = "metadata"
            st_app.rerun()
