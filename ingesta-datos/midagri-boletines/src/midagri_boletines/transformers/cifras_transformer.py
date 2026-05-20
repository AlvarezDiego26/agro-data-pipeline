import io
import hashlib
from datetime import date
from pathlib import Path
from zipfile import ZipFile
import polars as pl
from loguru import logger


def _sheet_to_rows(frame: pl.DataFrame, sheet_name: str) -> pl.DataFrame:
    """Transforma una hoja de cálculo en una representación larga des-pivotada de celdas."""
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


def read_supported_excel_bytes(content_bytes: bytes) -> pl.DataFrame:
    """Lee todas las hojas de un archivo Excel en bytes y las normaliza a celdas des-pivotadas."""
    try:
        sheets = pl.read_excel(content_bytes, sheet_id=0, has_header=False, engine="calamine", raise_if_empty=False)
        if isinstance(sheets, dict):
            frames: list[pl.DataFrame] = []
            for sheet_name, frame in sheets.items():
                try:
                    frames.append(_sheet_to_rows(frame, sheet_name))
                except Exception as e:
                    logger.warning(f"Error procesando hoja {sheet_name}: {e}")
            if not frames:
                return pl.DataFrame()
            return pl.concat(frames, how="diagonal_relaxed")
        return _sheet_to_rows(sheets, "Sheet1")
    except Exception as e:
        logger.warning(f"Error general leyendo libro de Excel: {e}")
        return pl.DataFrame()


def _with_lineage(
    raw_df: pl.DataFrame,
    source_name: str,
    member_name: str | None,
    file_hash: str,
    file_size_bytes: int,
    remote_signature: str,
    publication_year: int | None,
) -> pl.DataFrame:
    """Añade columnas de linaje de datos y un hash de registro único para control."""
    lineage_df = raw_df.with_columns(
        pl.lit(source_name).alias("archivo_origen"),
        pl.lit(member_name).alias("archivo_miembro"),
        pl.lit(Path(source_name).suffix.lower()).alias("tipo_archivo_origen"),
        pl.lit(publication_year).cast(pl.Int32, strict=False).alias("archivo_anio_publicacion"),
        pl.lit(date.today().isoformat()).alias("archivo_fecha_descarga"),
        pl.lit(str(publication_year or "")).alias("anio_publicacion"),
        pl.lit(file_hash).alias("archivo_hash"),
        pl.lit(file_size_bytes).cast(pl.Int64, strict=False).alias("archivo_tamano_bytes"),
        pl.lit(remote_signature).alias("archivo_firma_remota"),
    )
    hash_columns = sorted(lineage_df.columns)
    return lineage_df.with_columns(
        pl.concat_str(
            [pl.col(column).cast(pl.Utf8, strict=False).fill_null("") for column in hash_columns],
            separator="|",
        ).hash().cast(pl.Utf8).alias("registro_hash_fuente")
    )


def process_monthly_file_bytes(
    content_bytes: bytes,
    file_name: str,
    publication_year: int | None,
    remote_signature: str,
) -> pl.DataFrame:
    """
    Procesa un archivo mensual (ZIP o Excel directo) y devuelve un DataFrame con linaje y deduplicación.
    Todo el procesamiento y descompresión se hace en memoria.
    """
    file_hash = hashlib.sha256(content_bytes).hexdigest()
    file_size_bytes = len(content_bytes)
    
    # Caso 1: Archivo ZIP
    if file_name.lower().endswith(".zip"):
        logger.info(f"Descomprimiendo en memoria e importando ZIP: {file_name}")
        frames: list[pl.DataFrame] = []
        with ZipFile(io.BytesIO(content_bytes)) as archive:
            for member_name in archive.namelist():
                member_ext = Path(member_name).suffix.lower()
                if member_ext in {".xlsx", ".xls"}:
                    logger.debug(f"Procesando miembro Excel en ZIP: {member_name}")
                    member_bytes = archive.read(member_name)
                    df = read_supported_excel_bytes(member_bytes)
                    if not df.is_empty():
                        df = _with_lineage(
                            df,
                            source_name=file_name,
                            member_name=member_name,
                            file_hash=file_hash,
                            file_size_bytes=file_size_bytes,
                            remote_signature=remote_signature,
                            publication_year=publication_year
                        )
                        frames.append(df)
        if not frames:
            logger.warning(f"El ZIP {file_name} no contenía archivos Excel soportados.")
            return pl.DataFrame()
        return pl.concat(frames, how="diagonal_relaxed")
        
    # Caso 2: Excel Directo (.xlsx / .xls)
    elif file_name.lower().endswith((".xlsx", ".xls")):
        logger.info(f"Importando Excel directo en memoria: {file_name}")
        df = read_supported_excel_bytes(content_bytes)
        if not df.is_empty():
            return _with_lineage(
                df,
                source_name=file_name,
                member_name=None,
                file_hash=file_hash,
                file_size_bytes=file_size_bytes,
                remote_signature=remote_signature,
                publication_year=publication_year
            )
        return pl.DataFrame()
        
    else:
        logger.warning(f"Formato de archivo no soportado para transformación: {file_name}")
        return pl.DataFrame()
