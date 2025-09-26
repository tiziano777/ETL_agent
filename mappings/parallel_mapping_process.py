import os
import json
import gzip
import pyarrow.parquet as pq
import pyarrow as pa
from typing import List, Dict, Any, Tuple, Callable
from concurrent.futures import ProcessPoolExecutor, as_completed

from tqdm import tqdm
from mappings.mapper import Mapper


def parse_input_path(input_path: str) -> List[str]:
    supported_extensions = ('.parquet', '.jsonl.gz', ".jsonl", ".json")
    files_to_process = []
    if os.path.isfile(input_path):
        if input_path.endswith(supported_extensions): files_to_process.append(input_path)
    elif os.path.isdir(input_path):
        for root, _, files in os.walk(input_path):
            for filename in files:
                if filename.endswith(supported_extensions): files_to_process.append(os.path.join(root, filename))
    else: 
        print(f"Errore: Il percorso di input '{input_path}' non è valido.")
    return files_to_process

def process_file(file_path: str, mapper_mapping: List, dst_schema: Dict[str, Any], output_path: str, file_index: int) -> Tuple[str, bool, int, Any]:
    mapper = Mapper()
    output_filename = os.path.splitext(os.path.basename(file_path))[0]
    parquet_filepath = os.path.join(output_path, f"{output_filename}_mapped_{file_index}.parquet")
    mapped_samples = []
    processed_count = 0
    try:
        if file_path.endswith('.parquet'):
            table = pq.read_table(file_path)
            dict_rows = table.to_pylist()  # <-- usa pylist per dict annidati
            for row in dict_rows:
                mapped_sample = mapper.apply_mapping(row, mapper_mapping, dst_schema)
                mapped_samples.append(mapped_sample)
                processed_count += 1
        elif file_path.endswith('.jsonl.gz'):
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                for line in f:
                    sample = json.loads(line)
                    mapped_sample = mapper.apply_mapping(sample, mapper_mapping, dst_schema)
                    mapped_samples.append(mapped_sample)
                    processed_count += 1
        elif file_path.endswith('.jsonl') or file_path.endswith('.json'):
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    sample = json.loads(line)
                    mapped_sample = mapper.apply_mapping(sample, mapper_mapping, dst_schema)
                    mapped_samples.append(mapped_sample)
                    processed_count += 1
        # Scrittura solo in formato Parquet, senza serializzazione stringa
        if mapped_samples:
            table = pa.Table.from_pylist(mapped_samples)
            pq.write_table(table, parquet_filepath)
        return (os.path.basename(file_path), True, processed_count, None)
    except Exception as e:
        print(f"Errore durante l'elaborazione del file {file_path}: {e}")
        return (os.path.basename(file_path), False, processed_count, e)


def run_parallel_mapping(input_path: str, output_path: str, mapping: List, dst_schema: Dict[str, Any], progress_callback: Callable[[float], None]) -> Dict[str, int]:
    files_to_process = parse_input_path(input_path)
    if not files_to_process:
        return {"total_files": 0, "successful_files": 0, "total_processed_samples": 0}
    os.makedirs(output_path, exist_ok=True)

    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = {
            executor.submit(process_file, file, mapping, dst_schema, output_path, idx): file
            for idx, file in enumerate(files_to_process)
        }
        total_files = len(files_to_process)
        successful_files = 0
        total_processed_samples = 0
        for i, future in enumerate(as_completed(futures)):
            filename, success, processed_count, error = future.result()
            progress_callback((i + 1) / total_files)
            if success:
                successful_files += 1
                total_processed_samples += processed_count
            else:
                print(f"Elaborazione del file {filename} fallita: {error}")
        return {
            "total_files": total_files,
            "successful_files": successful_files,
            "total_processed_samples": total_processed_samples,
        }

