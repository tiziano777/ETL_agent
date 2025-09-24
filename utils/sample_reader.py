import os
import json
import gzip
import pandas as pd
import pyarrow.parquet as pq
import numpy as np

import re

def extract_tags_and_truncate(text: str, max_len: int = 120):
    """
    Estrae i contenuti dei tag <tag>...</tag> come chiavi allo stesso livello,
    rimuove il contenuto dei tag dal value originale e tronca i testi troppo lunghi.
    
    Args:
        text (str): stringa originale
        max_len (int): lunghezza massima dei testi
    
    Returns:
        Tuple[str, dict]: (value_troncato_senza_tag, dict_di_tag_troncati)
    """
    extracted = {}

    if text is None:
        return None, extracted

    # Pattern per catturare <tag>...</tag>
    tag_pattern = re.compile(r"<([a-zA-Z0-9_]+)>(.*?)</\1>", re.DOTALL)

    def replace_tag(match):
        tag_name = match.group(1)
        inner_text = match.group(2).strip()
        if len(inner_text) > max_len:
            inner_text = inner_text[:max_len] + "..."
        extracted[tag_name] = inner_text
        return ""  # rimuove il tag dal value originale

    # Rimuove tutti i tag dal testo
    cleaned_value = tag_pattern.sub(replace_tag, text).strip()

    # Tronca il value pulito se troppo lungo
    if len(cleaned_value) > max_len:
        cleaned_value = cleaned_value[:max_len] + "..."

    return cleaned_value, extracted

def truncate_strings(obj, max_len=120):
    """
    Ricorsiva: applica estrazione tag e troncamento su tutti i campi stringa,
    gestisce dict, list e valori primitivi, senza creare ridondanza di `value`.
    
    Args:
        obj: dict, list o valore primitivo
        max_len: lunghezza massima dei testi
    
    Returns:
        dict/list/valore troncato con tag estratti allo stesso livello
    """
    if obj is None:
        return None

    elif isinstance(obj, str):
        value, tags = extract_tags_and_truncate(obj, max_len=max_len)
        if tags:
            result = tags
            if value:
                result["value"] = value
            return result
        else:
            return value

    elif isinstance(obj, list):
        return [truncate_strings(x, max_len=max_len) for x in obj]

    elif isinstance(obj, dict):
        new_dict = {}
        for k, v in obj.items():
            processed = truncate_strings(v, max_len=max_len)
            if isinstance(processed, dict):
                # Merge dei tag estratti con eventuale value troncato
                if "value" in processed and not isinstance(v, str):
                    # value non originale → solo tag
                    for key, val in processed.items():
                        new_dict[key] = val
                else:
                    for key, val in processed.items():
                        new_dict[key] = val
            else:
                new_dict[k] = processed
        return new_dict

    else:
        # int, float, bool, None → ritorna così com'è
        return obj

def make_serializable(obj):
    """Converte ndarray e tipi numpy in tipi Python serializzabili in JSON."""
    if isinstance(obj, np.ndarray):
        return [make_serializable(x) for x in obj.tolist()]
    elif isinstance(obj, np.generic):  # np.int64, np.float32, ecc.
        return obj.item()
    elif isinstance(obj, dict):
        return {k: make_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_serializable(x) for x in obj]
    else:
        return obj

def load_dataset_samples(data_folder, k=1):
    """
    Cerca un file di dati supportato nella cartella specificata e ne estrae un campione JSON-serializzabile.
    
    Args:
        data_folder (str): Il percorso della sottocartella 'data'.
        k (int): Il numero di campioni da estrarre.

    Returns:
        list: Una lista di dizionari JSON-safe che rappresentano i campioni,
              o None se non vengono trovati file.
    """
    if not os.path.isdir(data_folder):
        print(f"La cartella dei dati '{data_folder}' non esiste.")
        return None

    supported_extensions = ['json', '.jsonl', '.csv', '.gz', '.parquet', '.jsonl.gz']
    data_files = [f for f in os.listdir(data_folder) if any(f.endswith(ext) for ext in supported_extensions)]
    
    if not data_files:
        print(f"Nessun file supportato trovato in '{data_folder}'.")
        return None
        
    file_path = os.path.join(data_folder, data_files[0])
    
    try:
        if file_path.endswith('.jsonl') or file_path.endswith('.json'):
            with open(file_path, 'r', encoding='utf-8') as f:
                samples = [json.loads(line) for line in f.readlines()[:k]]
        
        elif file_path.endswith('.jsonl.gz') or file_path.endswith('.gz'):
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                samples = [json.loads(line) for line in f.readlines()[:k]]
        
        elif file_path.endswith('.csv'):
            df = pd.read_csv(file_path, nrows=k)
            samples = df.to_dict('records')
        
        elif file_path.endswith('.parquet'):
            table = pq.read_table(file_path, columns=None)
            df = table.to_pandas().head(k)
            samples = df.to_dict('records')
        
        elif file_path.endswith('.tsv'):
            df = pd.read_csv(file_path, sep='\t', nrows=k)
            samples = df.to_dict('records')
        
        else:
            return None

        # 🔑 Normalizza i campioni per renderli JSON-serializzabili
        samples = [make_serializable(s) for s in samples]
        samples = [truncate_strings(s, max_len=120) for s in samples]  # tronca i testi lunghi

        return samples

    except Exception as e:
        print(f"Errore nel caricamento del campione da {file_path}: {e}")
        return None
