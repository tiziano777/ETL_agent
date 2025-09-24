import pandas as pd

dataset_path = "hf://datasets/glaiveai/RAG-v1/glaive_rag_v1.json"

# Legge il json remoto direttamente
df = pd.read_json(dataset_path)

# Estrai solo la prima riga
first_row = df.iloc[[0]]

# Salva in un nuovo file json
first_row.to_json("first_sample.json", orient="records", lines=True)

print("✅ Primo sample salvato in first_sample.json")


# 3. Prende solo il primo sample
first_row = df.iloc[[0]]

# 4. Salva in un nuovo file json
first_row.to_json("first_sample.json", orient="records", lines=True)

print("✅ Primo sample salvato in first_sample.json")
