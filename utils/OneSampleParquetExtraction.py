import pandas as pd

dataset_path = "hf://datasets/open-thoughts/OpenThoughts3-1.2M/data/train-00000-of-00120.parquet"

# Legge il parquet remoto direttamente
df = pd.read_parquet(dataset_path)

# Estrai solo la prima riga
first_row = df.iloc[[0]]

# Salva in un nuovo file parquet
first_row.to_parquet("first_sample.parquet", index=False)

print("✅ Primo sample salvato in first_sample.parquet")


# 3. Prende solo il primo sample
first_row = df.iloc[[0]]

# 4. Salva in un nuovo file parquet
first_row.to_parquet("first_sample.parquet", index=False)

print("✅ Primo sample salvato in first_sample.parquet")
