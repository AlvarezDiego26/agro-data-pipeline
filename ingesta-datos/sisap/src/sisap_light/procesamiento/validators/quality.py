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

