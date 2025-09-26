import os
import json
import pyarrow.parquet as pq
import jsonschema
import glob
import duckdb
import numpy as np  
from pathlib import Path
import pyarrow as pa # CORRECT: Import pyarrow core module

def show_query_dataset(st, base_path, processed_data_dir, metadata_path, schema_dir):

    st.header("Interrogazione Dataset")
    dataset_data = os.path.join(
        processed_data_dir,
        st.session_state.selected_version,
        st.session_state.selected_dataset_name,
        st.session_state.selected_subpath,
    )

    src_dataset_path = os.path.join(
        base_path,
        st.session_state.selected_version,
        st.session_state.selected_dataset_name
    )

    metadata_file = max(glob.glob(os.path.join(metadata_path,f"{st.session_state.selected_version}__{st.session_state.selected_dataset_name}__{st.session_state.selected_subpath}__*.json")))
    
    if not os.path.exists(metadata_file):
        st.error(f"Metadata file not found at {metadata_file}. Please generate the mapping first.")
        st.session_state.update(current_stage="action_selection")
        st.rerun()

    with open(metadata_file, "r") as f:
        st.session_state.metadata = json.load(f)

    dst_schema_id = st.session_state.metadata.get("dst_schema_id")
    if not dataset_data or not os.path.isdir(dataset_data):
        st.warning("Nessun dataset selezionato o percorso non valido.")
        return
    files = os.listdir(dataset_data)
    if not files:
        st.warning("Nessun file trovato nella cartella del dataset.")
        return
    first_file = files[0]
    file_path = os.path.join(dataset_data, first_file)
    file_ext = os.path.splitext(first_file)[-1].lower()
    valid_for_query = False
    if file_ext == ".parquet":
        try:
            
            table = pq.read_table(file_path)
            rows = table.to_pylist()
            if rows:
                st.subheader("Sample dal file Parquet")
                sample = rows[0]
                st.json(sample) 
            else:
                st.warning("Il file Parquet è vuoto.") 
            if not dst_schema_id:
                st.error("Il campo dst_schema_id è vuoto. Non è possibile interrogare il dataset.")
            else:
                schema_path = os.path.join(schema_dir, dst_schema_id)
                if not os.path.exists(schema_path):
                    st.error(f"Schema di destinazione '{dst_schema_id}' non trovato.")
                else:
                    with open(schema_path, "r", encoding="utf-8") as f:
                        schema = json.load(f)
                    validator = jsonschema.Draft7Validator(schema)
                    try:
                        validator.validate(sample)
                        st.success("Il dataset è elegibile per interrogazioni.")
                        valid_for_query = True

                        ##############################################################
                        # Funzione modulare per interrogare il dataset
                        ##############################################################
                        query_dataset(st,dataset_data,schema_path, src_dataset_path)
                        ##############################################################

                    except jsonschema.exceptions.ValidationError:
                        st.error("Il sample non è valido rispetto allo schema di destinazione.")
        except Exception as e:
            st.error(f"Errore nella lettura/parquet/validazione: {e}")
    if not valid_for_query:
        st.warning(f"Il formato dei file è {file_ext}. A noi serve che siano .parquet per eseguire interrogazioni in modo efficiente. Per favorw estrai un mapping e poi convertili nella sezione parallel dataset mapping.")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Genera Mapping"):
                st.session_state.current_stage = "select_target_schema"
                st.rerun()
        with col2:
            if st.button("Converti Dataset con Mapping esistente"):
                st.session_state.current_stage = "run_parallel_mapping"
                st.rerun()

def query_dataset(st, dataset_path, schema_path, output_path):
    """
    Funzione modulare per interrogare dataset in formato Parquet usando DuckDB
    
    Args:
        st: Modulo Streamlit
        dataset_path: Path alla cartella contenente i file Parquet
        schema_path: Path al file JSON Schema del dataset
        output_path: Path base per il salvataggio dei risultati (proveniente da src_dataset_path)
    """
    st.subheader("🔍 Interrogazione Dataset")
    
    # Verifica che i path esistano
    if not os.path.exists(dataset_path):
        st.error(f"❌ Percorso dataset non trovato: {dataset_path}")
        return
    
    if not os.path.exists(schema_path):
        st.error(f"❌ Percorso schema non trovato: {schema_path}")
        return
    
    # Layout a due colonne per i pulsanti principali
    col1, col2 = st.columns(2)
    
    # --- PULSANTE 1: VISUALIZZA SCHEMA ---
    with col1:
        if st.button("📋 Visualizza Schema Dataset", key="btn_show_schema"):
            # Aggiorna lo stato e nasconde l'interfaccia query se attiva
            st.session_state.update(show_schema=True, show_query_interface=False)
            st.rerun()
    
    # --- PULSANTE 2: QUERY SQL ---
    with col2:
        if st.button("💻 Esegui Query SQL", key="btn_show_query"):
            # Aggiorna lo stato e nasconde lo schema se attivo
            st.session_state.update(show_query_interface=True, show_schema=False)
            st.rerun()
    
    # --- VISUALIZZAZIONE SCHEMA ---
    if st.session_state.get('show_schema', False):
        st.markdown("---")
        st.subheader("📋 Schema del Dataset")
        
        try:
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)
            
            # Visualizza lo schema in formato JSON con syntax highlighting
            st.json(schema)
            
            # Opzione per nascondere lo schema - USO ON_CLICK
            if st.button("🔼 Nascondi Schema", key="btn_hide_schema", on_click=lambda: st.session_state.update(show_schema=False)):
                st.rerun() # Rerun per nascondere immediatamente
                
        except Exception as e:
            st.error(f"❌ Errore nella lettura dello schema: {str(e)}")
    
    # --- INTERFACCIA QUERY SQL ---
    if st.session_state.get('show_query_interface', False):
        st.markdown("---")
        st.subheader("💻 Interfaccia Query SQL")
        
        # Lista i file Parquet disponibili
        parquet_files = list(Path(dataset_path).glob("*.parquet"))
        
        if not parquet_files:
            st.warning("⚠️ Nessun file Parquet trovato nella cartella specificata")
            return
        
        # Mostra informazioni sui file disponibili
        with st.expander("📁 File Parquet Disponibili", expanded=False):
            for file in parquet_files:
                st.write(f"• `{file.name}`")
        
        # Area di testo per la query SQL
        st.markdown("**Scrivi la tua query SQL:**")
        
        # Query di esempio
        example_query = f"""-- Esempio di query
            SELECT * FROM '{dataset_path}/*.parquet' 
            LIMIT 10;"""
        
        query = st.text_area(
            "Query SQL",
            value=st.session_state.get('last_query', example_query), # Mantieni l'ultima query
            height=150,
            key="query_text_area",
            help="Scrivi la tua query SQL. Puoi usare il path completo ai file Parquet o il pattern con wildcard."
        )
        
        # Parametri di esecuzione
        col_exec1, col_exec2 = st.columns([3, 1])
        
        with col_exec1:
            limit_results = st.number_input(
                "Limite risultati da visualizzare", 
                min_value=1, 
                max_value=10000, 
                value=100,
                key="limit_results_input",
                help="Numero massimo di righe da mostrare nei risultati"
            )
        
        # Callback per l'esecuzione della query
        def execute_query_callback():
            st.session_state.run_query = True
            st.session_state.last_query = st.session_state.query_text_area # Salva la query
            
        with col_exec2:
            st.write("")  # Spacer
            st.write("")  # Spacer
            execute_query = st.button("▶️ Esegui Query", type="primary", key="btn_execute_query", on_click=execute_query_callback)
        
        # Esecuzione della query
        if st.session_state.get('run_query', False):
            # Assicurati di resettare lo stato per evitare riesecuzioni infinite in caso di rerun
            st.session_state.run_query = False 
            
            try:
                # Inizializza connessione DuckDB
                with duckdb.connect() as conn:
                    st.info("🔄 Esecuzione query in corso...")
                    
                    # Esegui la query
                    query_to_execute = st.session_state.last_query
                    result = conn.execute(query_to_execute).fetchdf()
                    
                    # --- APPLICAZIONE SOLUZIONE PRECEDENTE (pulizia tipi 'object') ---
                    for col in result.columns:
                        if result[col].dtype == 'object':
                            try:
                                # Converte la colonna object in stringa per compatibilità PyArrow/Streamlit
                                result[col] = result[col].astype(str)
                            except Exception as e:
                                st.warning(f"⚠️ Impossibile forzare la colonna '{col}' a stringa per la visualizzazione: {e}")
                    # --- FINE SOLUZIONE PRECEDENTE ---
                    
                    # Limita i risultati se necessario
                    if len(result) > limit_results:
                        result_limited = result.head(limit_results)
                        st.warning(f"⚠️ Visualizzati solo i primi {limit_results} risultati su {len(result)} totali")
                        result_to_show = result_limited
                    else:
                        result_to_show = result
                    
                    # Mostra i risultati
                    st.success(f"✅ Query eseguita con successo! Trovate {len(result)} righe.")
                    
                    # Statistiche sui risultati
                    col_stat1, col_stat2, col_stat3 = st.columns(3)
                    with col_stat1:
                        st.metric("Righe totali", len(result))
                    with col_stat2:
                        st.metric("Colonne", len(result.columns))
                    with col_stat3:
                        st.metric("Righe visualizzate", len(result_to_show))
                    
                    # Visualizza la tabella dei risultati
                    st.dataframe(
                        result_to_show, 
                        height=400
                    )
                    
                    # *************************************************************
                    # --- INIZIO MODIFICHE PER "OPZIONI AGGIUNTIVE" ---
                    # *************************************************************
                    
                    # Opzioni aggiuntive
                    with st.expander("📊 Opzioni Aggiuntive", expanded=False):
                        
                        # Definisco lo stato per i pulsanti qui, per poterlo usare sotto
                        if 'show_info' not in st.session_state:
                            st.session_state.show_info = False
                        if 'show_save_result' not in st.session_state:
                            st.session_state.show_save_result = False
                            
                        col_opt1, col_opt2, col_opt3 = st.columns(3)
                        
                        with col_opt1:
                            # Download CSV
                            csv = result_to_show.to_csv(index=False)
                            st.download_button(
                                label="📥 Scarica risultati (CSV)",
                                data=csv,
                                file_name="query_results.csv",
                                mime="text/csv",
                                key="download_csv_opt"
                            )
                        
                        with col_opt2:
                            # ℹ️ Info Dataset - USO ON_CLICK per impostare lo stato
                            st.button(
                                "ℹ️ Info Dataset", 
                                key="info_dataset_opt",
                                on_click=lambda: st.session_state.update(show_info=True, show_save_result=False) # Nasconde l'altro
                            )
                            
                        with col_opt3:
                            # 💾 Salva Risultati - USO ON_CLICK per impostare lo stato
                            st.button(
                                "💾 Salva Risultati", 
                                key="save_results_opt",
                                on_click=lambda: st.session_state.update(show_save_result=True, show_info=False) # Nasconde l'altro
                            )
                            
                        # --- Info Dataset ---
                        if st.session_state.get('show_info', False):
                            st.markdown("---")
                            st.write("**Informazioni sui tipi di dati:**")
                            # Usa result_to_show.dtypes.to_frame("Tipo").astype(str) se l'errore persiste qui
                            st.dataframe(result_to_show.dtypes.to_frame("Tipo")) 
                            
                            # Usa on_click per chiudere
                            if st.button("🔼 Chiudi Info Dataset", key="close_info_opt", on_click=lambda: st.session_state.update(show_info=False)):
                                st.rerun() # Rerun necessario per aggiornare la visualizzazione
                                
                        # --- Salva Risultati ---
                        if st.session_state.get('show_save_result', False):
                            st.markdown("---")
                            st.subheader("💾 Salva Risultati Query")
                            
                            folder_name = st.text_input(
                                "Nome della cartella di destinazione",
                                placeholder="inserisci_nome_cartella",
                                key="folder_name_input", # Aggiungo una key
                                help="Inserisci il nome della cartella dove salvare i risultati della query"
                            )
                            
                            col_save1, col_save2, col_save3 = st.columns([2, 1, 1])
                            
                            # Callback di salvataggio
                            def confirm_save_callback(df, path, name):
                                if name.strip():
                                    # La funzione save_query_results stampa messaggi di successo/errore
                                    if save_query_results(st, df, path, name.strip()):
                                        st.session_state.show_save_result = False
                                        st.rerun() # Forza il rerun dopo il salvataggio
                                else:
                                    # Usa uno stato temporaneo per mostrare l'errore di validazione
                                    st.session_state.save_error_msg = "❌ Inserisci un nome valido per la cartella"
                                    
                            with col_save1:
                                st.write("")
                                if st.session_state.get('save_error_msg'):
                                    st.error(st.session_state.save_error_msg)
                                    del st.session_state.save_error_msg
                                    
                            with col_save2:
                                if st.button("💾 Conferma Salvataggio", type="primary", key="confirm_save_opt", 
                                             on_click=lambda: confirm_save_callback(result_to_show, output_path, st.session_state.folder_name_input)):
                                    pass
                                    
                            with col_save3:
                                # Usa on_click per chiudere
                                if st.button("❌ Annulla", key="cancel_save_opt", on_click=lambda: st.session_state.update(show_save_result=False)):
                                    st.rerun() # Forza il rerun per nascondere
                    
                    # *************************************************************
                    # --- FINE MODIFICHE PER "OPZIONI AGGIUNTIVE" ---
                    # *************************************************************
                                       
            except Exception as e:
                st.error(f"❌ Errore nell'esecuzione della query: {str(e)}")
                
                # Suggerimenti per errori comuni
                if "no such file" in str(e).lower():
                    st.info("💡 **Suggerimento**: Verifica che il path ai file Parquet sia corretto")
                elif "syntax error" in str(e).lower():
                    st.info("💡 **Suggerimento**: Controlla la sintassi SQL della query")
        
        # Opzione per nascondere l'interfaccia query - USO ON_CLICK
        if st.button("🔼 Nascondi Interfaccia Query", key="btn_hide_query", on_click=lambda: st.session_state.update(show_query_interface=False)):
            st.rerun()
    
    # --- PULSANTI AGGIUNTIVI ---
    st.markdown("---")
    st.subheader("🔧 Funzionalità Aggiuntive")
    
    # Layout per pulsanti aggiuntivi (migliorato, centrato e distanziato)
    col_sp1, col_add1, col_sp2, col_add2, col_sp3, col_add3, col_sp4 = st.columns([0.2, 1, 0.2, 1, 0.2, 1, 0.2])
    
    # Uso ON_CLICK per tutti i pulsanti aggiuntivi
    with col_add1:
        st.button("📈 Statistiche Dataset", key="btn_stats", on_click=lambda: st.session_state.update(show_stats=True, show_preview=False, show_structure=False))
    with col_add2:
        st.button("🔍 Anteprima Dati", key="btn_preview", on_click=lambda: st.session_state.update(show_preview=True, show_stats=False, show_structure=False))
    with col_add3:
        st.button("🏗️ Struttura Tabelle", key="btn_structure", on_click=lambda: st.session_state.update(show_structure=True, show_stats=False, show_preview=False))
        
    # Azioni pulsanti
    # Rerun è necessario per la prima attivazione se i pulsanti sono attivati in un'area
    # diversa da quella del main-script
    if st.session_state.get("show_stats", False):
        st.markdown("---")
        show_dataset_stats(st, dataset_path)
        if st.button("🔼 Chiudi Statistiche", key="close_stats", on_click=lambda: st.session_state.update(show_stats=False)):
            st.rerun()
            
    if st.session_state.get("show_preview", False):
        st.markdown("---")
        show_data_preview(st, dataset_path)
        if st.button("🔼 Chiudi Anteprima", key="close_preview", on_click=lambda: st.session_state.update(show_preview=False)):
            st.rerun()
            
    if st.session_state.get("show_structure", False):
        st.markdown("---")
        show_table_structure(st, dataset_path)
        if st.button("🔼 Chiudi Struttura", key="close_structure", on_click=lambda: st.session_state.update(show_structure=False)):
            st.rerun()
            
    st.markdown("---")
    # Pulsanti di navigazione in fondo, centrati
    col_nav_sp1, col_home, col_nav_sp2, col_sel, col_nav_sp3 = st.columns([0.5, 1, 0.2, 1, 0.5])
    
    # Uso ON_CLICK
    with col_home:
        st.button("Torna alla home", key="home_btn_nav", on_click=lambda: st.session_state.update(current_stage="action_selection"))
    with col_sel:
        # Uso ON_CLICK
        st.button("Torna alla selezione del dataset", use_container_width=True, key="sel_btn_nav", 
                  on_click=lambda: st.session_state.update(current_stage="dataset_selection", metadata_confirmed=False, pipeline_started=False, src_schema=None, dst_schema=None))

##############################################################
# Funzioni di supporto per funzionalità aggiuntive
##############################################################

def show_dataset_stats(st, dataset_path):
    """Mostra statistiche generali del dataset"""
    try:
        with duckdb.connect() as conn:
            query = f"SELECT COUNT(*) as total_rows FROM '{dataset_path}/*.parquet'"
            result = conn.execute(query).fetchone()
            
            st.success(f"📊 **Statistiche Dataset**")
            st.metric("Righe totali", f"{result[0]:,}")
            
            # Dimensione file
            total_size = sum(f.stat().st_size for f in Path(dataset_path).glob("*.parquet"))
            st.metric("Dimensione totale", f"{total_size / (1024*1024):.2f} MB")
            
    except Exception as e:
        st.error(f"Errore nel calcolo delle statistiche: {str(e)}")

def show_data_preview(st, dataset_path):
    """Mostra un'anteprima dei dati"""
    try:
        with duckdb.connect() as conn:
            query = f"SELECT * FROM '{dataset_path}/*.parquet' LIMIT 5"
            result = conn.execute(query).fetchdf()
            
            st.success("👀 **Anteprima Dati (prime 5 righe)**")
            st.dataframe(result)
            
    except Exception as e:
        st.error(f"Errore nella visualizzazione dell'anteprima: {str(e)}")

def show_table_structure(st, dataset_path):
    """Mostra la struttura delle tabelle"""
    try:
        with duckdb.connect() as conn:
            query = f"DESCRIBE SELECT * FROM '{dataset_path}/*.parquet' LIMIT 1"
            result = conn.execute(query).fetchdf()
            
            st.success("🏗️ **Struttura Tabelle**")
            st.dataframe(result)
            
    except Exception as e:
        st.error(f"Errore nella visualizzazione della struttura: {str(e)}")

def save_query_results(st, result_df, output_path, folder_name):
    """
    Salva i risultati della query in file Parquet con dimensione massima di 120MB,
    preservando i tipi di dati annidati.
    """
    
    # Crea una copia del DataFrame per le modifiche
    df_to_save = result_df.copy() 
    
    # -----------------------------------------------------------------
    # CORREZIONE: Tentativo di conversione PyArrow con fallback JSON
    # -----------------------------------------------------------------
    
    try:
        # TENTA 1: Converti l'intero DataFrame direttamente in una PyArrow Table.
        # FIX APPLICATO: Usa pa.Table
        table = pa.Table.from_pandas(df_to_save, preserve_index=False)
        st.info("✅ Tipi annidati inferiti e gestiti con successo da PyArrow.")
        
    # FIX APPLICATO: Usa pa.ArrowInvalid (invece di pq.lib.ArrowInvalid)
    except pa.ArrowInvalid as e: 
        # TENTA 2: Se l'inferenza fallisce, significa che ci sono tipi inconsistenti o troppo complessi.
        st.warning(f"⚠️ PyArrow non è riuscito a inferire lo schema. Tentativo di serializzazione JSON per le colonne problematiche. Errore originale: {e}")
        
        # Serializza esplicitamente ogni colonna 'object' in JSON string
        for col in df_to_save.columns:
            if df_to_save[col].dtype == 'object':
                try:
                    # Serializza l'oggetto Python complesso (dict, list, ecc.) in una stringa JSON
                    # `json.dumps(x)` per ogni elemento.
                    df_to_save[col] = df_to_save[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if x is not None else None)
                    st.info(f"Colonna '{col}' serializzata in stringa JSON.")
                except Exception as json_e:
                    st.error(f"❌ Errore FATALE: Impossibile convertire la colonna '{col}' a stringa o JSON: {json_e}")
                    raise # Blocca il processo se la serializzazione JSON fallisce
                    
        # Riprova la conversione a PyArrow Table con i dati serializzati in JSON (che PyArrow gestisce come stringhe)
        # FIX APPLICATO: Usa pa.Table
        table = pa.Table.from_pandas(df_to_save, preserve_index=False)
    
    # -----------------------------------------------------------------

    try:
        # Crea il percorso completo della cartella di destinazione (logica omessa per brevità)
        destination_path = Path(output_path) / folder_name
        
        if destination_path.exists():
            if any(destination_path.iterdir()):
                st.error(f"❌ La cartella '{folder_name}' esiste già e non è vuota!")
                return False
        else:
            destination_path.mkdir(parents=True, exist_ok=True)
            st.info(f"📁 Creata nuova cartella: {destination_path}")
        
        # Usa le dimensioni della PyArrow Table per il calcolo
        estimated_size_bytes = table.nbytes
        estimated_size_mb = estimated_size_bytes / (1024 * 1024)
        st.info(f"📊 Dimensione stimata dei dati: {estimated_size_mb:.2f} MB")
        
        # Dimensione massima per file (120 MB)
        max_size_mb = 120
        
        # Salva direttamente la PyArrow Table, che è più efficiente
        if estimated_size_mb <= max_size_mb:
            output_file = destination_path / "query_results_001.parquet"
            # USA PYARROW PER SALVARE LA TABELLA
            pq.write_table(table, output_file) 
            st.success(f"✅ Risultati salvati in: `{output_file}`")
            st.info(f"📈 Salvate {table.num_rows} righe in 1 file")
        else:
            # Salvataggio in chunk (più complesso con PyArrow, ma necessario)
            num_chunks = int(np.ceil(estimated_size_mb / max_size_mb))
            chunk_size = int(np.ceil(table.num_rows / num_chunks))
            
            st.info(f"🔧 Dataset grande, suddiviso in {num_chunks} file da massimo {max_size_mb}MB")
            
            progress_bar = st.progress(0)
            
            for i in range(num_chunks):
                start_idx = i * chunk_size
                end_idx = min(start_idx + chunk_size, table.num_rows)
                
                # Slicing della PyArrow Table
                chunk_table = table.slice(offset=start_idx, length=end_idx - start_idx)
                
                file_name = f"query_results_{i+1:03d}.parquet"
                output_file = destination_path / file_name
                
                # Salva il chunk
                pq.write_table(chunk_table, output_file)
                
                progress_bar.progress((i + 1) / num_chunks)
            
            progress_bar.progress(1.0)
            st.success(f"✅ Salvataggio completato!")
            st.info(f"📈 Salvate {table.num_rows} righe totali in {num_chunks} file")
        
        st.session_state.show_save_interface = False
        return True
        
    except Exception as e:
        st.error(f"❌ Errore durante il salvataggio: {str(e)}")
        return False