import polars as pl


def normalize_columns(df: pl.DataFrame) -> pl.DataFrame:
    renamed = {col: col.strip().lower().replace(' ', '_') for col in df.columns}
    return df.rename(renamed)
