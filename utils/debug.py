
# --- DEBUG SESSION STATE ---
def show_debug_session_state(st): 
    st.markdown('---')
    st.subheader('🛠️ Debug: Variabili di sessione Streamlit')
    st.json({k: v for k, v in st.session_state.items()})