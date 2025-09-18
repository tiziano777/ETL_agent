import os
import json
import gzip
import pandas as pd

import os
import json
import gzip
import pandas as pd

def load_dataset_samples(data_folder, k=1):
    """
    Cerca un file di dati supportato nella cartella specificata e ne estrae un campione.
    
    Args:
        data_folder (str): Il percorso della sottocartella 'data'.
        k (int): Il numero di campioni da estrarre.

    Returns:
        list: Una lista di dizionari che rappresenta i campioni, o None se non vengono trovati file.
    """
    if not os.path.isdir(data_folder):
        print(f"La cartella dei dati '{data_folder}' non esiste.")
        return None

    # Elenco delle estensioni supportate
    supported_extensions = ['.jsonl', '.csv', '.gz', '.parquet', '.jsonl.gz']
    
    # Cerca il primo file supportato
    data_files = [f for f in os.listdir(data_folder) if any(f.endswith(ext) for ext in supported_extensions)]
    
    if not data_files:
        print(f"Nessun file supportato trovato in '{data_folder}'.")
        return None
        
    file_path = os.path.join(data_folder, data_files[0])
    
    try:
        if file_path.endswith('.jsonl'):
            with open(file_path, 'r', encoding='utf-8') as f:
                samples = [json.loads(line) for line in f.readlines()[:k]]
            return samples
        
        elif file_path.endswith('.jsonl.gz') or file_path.endswith('.gz'):
            # Usa il modulo gzip per leggere il file compresso
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                samples = [json.loads(line) for line in f.readlines()[:k]]
            return samples
        
        elif file_path.endswith('.csv'):
            # Pandas gestisce i file .csv
            df = pd.read_csv(file_path, nrows=k)
            return df.to_dict('records')
        
        elif file_path.endswith('.parquet'):
            # Pandas legge i file Parquet
            df = pd.read_parquet(file_path, nrows=k)
            return df.to_dict('records')
        
        elif file_path.endswith('.tsv'):
            df = pd.read_csv(file_path, sep='\t', nrows=k)
            return df.to_dict('records')

    except Exception as e:
        print(f"Errore nel caricamento del campione da {file_path}: {e}")
        return None
    
    return None