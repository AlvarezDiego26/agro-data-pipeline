from __future__ import annotations

import shutil
from pathlib import Path
from zipfile import ZipFile

import polars as pl


def extract_supported_zip_members(zip_path: Path) -> tuple[Path, list[Path]]:
    temp_dir = zip_path.parent / f"{zip_path.stem}_tmp"
    if temp_dir.exists():
        shutil.rmtree(temp_dir, ignore_errors=True)
    temp_dir.mkdir(parents=True, exist_ok=True)

    members: list[Path] = []
    with ZipFile(zip_path) as archive:
        archive.extractall(temp_dir)
    for path in temp_dir.rglob("*"):
        if path.is_file() and path.suffix.lower() in {".xlsx", ".xls"}:
            members.append(path)
    return temp_dir, sorted(members)


def read_supported_file(source_path: Path) -> pl.DataFrame:
    # `sheet_id=0` forces Polars to return every worksheet as a dict.
    sheets = pl.read_excel(source_path, sheet_id=0, has_header=False)
    if isinstance(sheets, dict):
        frames: list[pl.DataFrame] = []
        for sheet_name, frame in sheets.items():
            frames.append(_sheet_to_rows(frame, sheet_name))
        if not frames:
            return pl.DataFrame()
        return pl.concat(frames, how="diagonal_relaxed")
    return _sheet_to_rows(sheets, "Sheet1")


def _sheet_to_rows(frame: pl.DataFrame, sheet_name: str) -> pl.DataFrame:
    if frame.is_empty():
        return pl.DataFrame(
            schema={
                "hoja_nombre": pl.Utf8,
                "fila_idx": pl.Int64,
                "columna_idx": pl.Int64,
                "columna_nombre": pl.Utf8,
                "celda_valor": pl.Utf8,
            }
        )

    column_map = {column: f"col_{idx + 1}" for idx, column in enumerate(frame.columns)}
    columns = [column_map[column] for column in frame.columns]
    normalized = (
        frame.rename(column_map)
        .with_row_index("fila_idx", offset=1)
    )
    melted = normalized.unpivot(
        index=["fila_idx"],
        on=columns,
        variable_name="columna_nombre",
        value_name="celda_valor",
    )
    return (
        melted.with_columns(
            pl.lit(sheet_name).alias("hoja_nombre"),
            pl.col("columna_nombre").str.replace("col_", "").cast(pl.Int64, strict=False).alias("columna_idx"),
            pl.col("celda_valor").cast(pl.Utf8, strict=False).fill_null(""),
        )
        .filter(pl.col("celda_valor").str.strip_chars() != "")
        .select(["hoja_nombre", "fila_idx", "columna_idx", "columna_nombre", "celda_valor"])
    )
