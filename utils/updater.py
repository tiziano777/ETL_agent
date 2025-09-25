import os
import json
import pandas as pd
import shutil
from datetime import datetime
import duckdb

import dotenv
dotenv.load_dotenv()

METADATA_PATH = os.getenv("METADATA_PATH")
METADATA_MASTER_PATH = os.getenv("METADATA_MASTER_PATH")
MASTER_PARQUET_FILE = os.path.join(METADATA_MASTER_PATH, "master_metadata.parquet")


def archive_and_update_metadata(st,metadata_file, metadata_json):
        # Archivia il file precedente
        archive_dir = os.path.join(METADATA_PATH, "archived_metadata")
        os.makedirs(archive_dir, exist_ok=True)
        if os.path.exists(metadata_file):
            archived_name = os.path.basename(metadata_file)
            archived_path = os.path.join(archive_dir, archived_name)
            shutil.copy2(metadata_file, archived_path)

        # Aggiorna il timestamp nel nome file
        base_name = f"{st.session_state.selected_version}__{st.session_state.selected_dataset_name}__{st.session_state.selected_subpath}__{datetime.now().strftime('%Y%m%d%H%M')}.json"
        new_metadata_file = os.path.join(METADATA_PATH, base_name)

        # Salva il nuovo file
        with open(new_metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata_json, f, indent=2, ensure_ascii=False)

        return new_metadata_file

def update_master_metadata(new_json_file_path: str):
    """
    Aggiorna il file master_metadata.parquet con i dati di un nuovo/modificato
    file JSON, usando 'doc_id' come chiave per l'UPSERT.
    Mantiene le colonne 'doc_id', 'metadata', 'src_schema' e 'mapping' come 
    stringhe JSON complete per una facile interrogazione.

    Args:
        new_json_file_path (str): Il percorso completo del nuovo file JSON.
    """
    print(f"[DEBUG] Chiamata update_master_metadata con file: {new_json_file_path}")
    # Verifica che il file JSON esista
    if not os.path.exists(new_json_file_path):
        print(f"[DEBUG] ERRORE: File non trovato: {new_json_file_path}")
        return

    print(f"[DEBUG] Elaborazione del file: {new_json_file_path}")

    # 1. Carica e prepara i dati JSON
    try:
        print("[DEBUG] Apro il file JSON...")
        with open(new_json_file_path, 'r') as f:
            new_data = json.load(f)
        print("[DEBUG] JSON caricato correttamente.")
        # Estrai e serializza le colonne richieste in stringhe JSON
        flat_record = {
            "doc_id": new_data.get("doc_id"),
            # Serializza i sotto-oggetti complessi in stringhe JSON
            "metadata": json.dumps(new_data.get("metadata", {})),
            "src_schema": json.dumps(new_data.get("src_schema", {})),
            "mapping": json.dumps(new_data.get("mapping", []))
        }
        print(f"[DEBUG] flat_record creato: {flat_record}")
        new_df = pd.DataFrame([flat_record])
        print("[DEBUG] DataFrame creato.")
    except Exception as e:
        print(f"[DEBUG] ERRORE nel caricamento/parsing del JSON: {e}")
        return

    # 2. Connessione a DuckDB in-memory
    print("[DEBUG] Connessione a DuckDB...")
    con = duckdb.connect(database=':memory:', read_only=False)
    con.register('new_metadata_view', new_df)
    print("[DEBUG] DataFrame registrato in DuckDB.")

    # 3. Se il file master non esiste, crealo direttamente
    if not os.path.exists(MASTER_PARQUET_FILE):
        print(f"[DEBUG] File master non trovato. Creazione di {MASTER_PARQUET_FILE}...")
        try:
            con.execute(f"COPY new_metadata_view TO '{MASTER_PARQUET_FILE}' (FORMAT PARQUET);")
            print(f"[DEBUG] Creazione del file master completata.")
        except Exception as e:
            print(f"[DEBUG] ERRORE durante la creazione iniziale del file Parquet: {e}")
        finally:
            print("[DEBUG] Chiudo la connessione DuckDB.")
            con.close()
        return

    # 4. Esegui l'operazione UPSERT sul file Parquet master
    try:
        temp_parquet_file = MASTER_PARQUET_FILE + ".tmp"
        print(f"[DEBUG] Percorso file temporaneo: {temp_parquet_file}")
        # Query che simula l'UPSERT: (Vecchi record NON aggiornati) UNION ALL (Nuovo/Aggiornato record)
        # Nota: La UNION ALL richiede che le colonne abbiano gli stessi nomi e tipi.
        update_query = f"""
            SELECT doc_id, metadata, src_schema, mapping
            FROM read_parquet('{MASTER_PARQUET_FILE}') AS old_data
            WHERE old_data.doc_id NOT IN (SELECT doc_id FROM new_metadata_view)
            UNION ALL
            SELECT doc_id, metadata, src_schema, mapping
            FROM new_metadata_view
        """
        print("[DEBUG] Eseguo la query di UPSERT su DuckDB...")
        con.execute(f"COPY ({update_query}) TO '{temp_parquet_file}' (FORMAT PARQUET);")
        print("[DEBUG] Query completata. Sostituisco il file master...")
        # 5. Sostituzione atomica del vecchio file con il nuovo
        os.replace(temp_parquet_file, MASTER_PARQUET_FILE)
        print(f"[DEBUG] Aggiornamento completato con successo. File master: {MASTER_PARQUET_FILE}")
    except Exception as e:
        print(f"[DEBUG] ERRORE durante l'operazione DuckDB/UPSERT: {e}")
        if os.path.exists(temp_parquet_file):
             os.remove(temp_parquet_file) # Pulizia in caso di fallimento
    finally:
        print("[DEBUG] Chiudo la connessione DuckDB.")
        con.close()

