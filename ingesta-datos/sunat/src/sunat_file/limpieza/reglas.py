import polars as pl

REQUIRED_MIN_COLUMNS = 3


def validate_non_empty(df: pl.DataFrame, name: str) -> None:
    if df.is_empty():
        raise ValueError(f"{name} no contiene filas.")

    if df.width < REQUIRED_MIN_COLUMNS:
        raise ValueError(f"{name} no contiene suficientes columnas utiles.")


def validate_expected_columns(df: pl.DataFrame, expected: list[str], name: str) -> None:
    faltantes = [col for col in expected if col not in df.columns]
    if faltantes:
        raise ValueError(f"{name} no contiene columnas esperadas: {faltantes}")


def normalize_dataset(df: pl.DataFrame, business_keys: list[str]) -> pl.DataFrame:
    if df.is_empty():
        return df

    exprs: list[pl.Expr] = []
    protected_keys = set(business_keys)

    for col, dtype in df.schema.items():
        if col in protected_keys:
            exprs.append(pl.col(col))
        elif dtype in (pl.Utf8, pl.Categorical, pl.String):
            exprs.append(pl.col(col).cast(pl.Utf8, strict=False).fill_null(""))
        elif dtype in (
            pl.Int8,
            pl.Int16,
            pl.Int32,
            pl.Int64,
            pl.UInt8,
            pl.UInt16,
            pl.UInt32,
            pl.UInt64,
            pl.Float32,
            pl.Float64,
        ):
            exprs.append(pl.col(col).fill_null(0))
        else:
            exprs.append(pl.col(col))

    return df.with_columns(exprs)
