import polars as pl
import os
from pathlib import Path

base_path = Path(r"c:\Users\diego\Documents\EMPRESA\agro-proyecto\ingesta-datos\sisap\data")

def check_zeros():
    files = []
    # Search for parquet files
    for root, dirs, filenames in os.walk(base_path):
        for f in filenames:
            if f.endswith('.parquet'):
                files.append(os.path.join(root, f))
    
    if not files:
        print("No se encontraron archivos parquet locales.")
        return

    print(f"Revisando {len(files)} archivos parquet...")
    
    total_rows = 0
    non_zero_rows = 0
    
    for f in files:
        try:
            df = pl.read_parquet(f)
            total_rows += len(df)
            
            if "volumen_ton" in df.columns:
                non_zero = df.filter(pl.col("volumen_ton") > 0)
                non_zero_rows += len(non_zero)
            elif "precio_prom" in df.columns:
                non_zero = df.filter(pl.col("precio_prom") > 0)
                non_zero_rows += len(non_zero)
                
        except Exception as e:
            pass
            
    print(f"Total de registros revisados: {total_rows}")
    print(f"Total de registros con valor mayor a 0: {non_zero_rows}")

if __name__ == "__main__":
    check_zeros()
