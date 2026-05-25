import polars as pl
import os
from pathlib import Path

base_path = Path(r"c:\Users\diego\Documents\EMPRESA\agro-proyecto\ingesta-datos\sisap\data")

def print_samples():
    files = []
    for root, dirs, filenames in os.walk(base_path):
        for f in filenames:
            if f.endswith('.parquet'):
                files.append(os.path.join(root, f))
    
    if not files:
        print("No se encontraron archivos parquet locales.")
        return

    print(f"Archivos encontrados: {len(files)}")
    for f in files[:5]:
        print(f"File: {f}")
        try:
            df = pl.read_parquet(f)
            if len(df) > 0:
                print(df.head(2).to_dicts())
        except Exception as e:
            print("Error", e)

if __name__ == "__main__":
    print_samples()
