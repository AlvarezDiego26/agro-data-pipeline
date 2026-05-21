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
MONTH_NAME_TO_NUMBER = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}


def _classify_member_name(member_name: str | None, source_name: str) -> tuple[str, str]:
    candidate = f"{member_name or ''} {source_name}".lower()
    patterns = [
        ("agricola", "produccion_agricola"),
        ("pecuario", "produccion_pecuaria_avicola"),
        ("avicola", "produccion_pecuaria_avicola"),
        ("agroindustria", "agroindustria"),
        ("comercio interno", "comercio_interno"),
        ("comercio- interno", "comercio_interno"),
        ("comercio externo", "comercio_exterior"),
        ("comercio- externo", "comercio_exterior"),
        ("insumos y servicios", "insumos_y_servicios_agrarios"),
        ("insumos-y-servicios", "insumos_y_servicios_agrarios"),
    ]
    for marker, category in patterns:
        if marker in candidate:
            return category, "agrario"
    return "otros_agrarios", "agrario"


def _extract_month_from_name(member_name: str | None, source_name: str) -> tuple[int | None, str | None]:
    candidate = f"{member_name or ''} {source_name}".lower()
    for month_name, month_number in MONTH_NAME_TO_NUMBER.items():
        if month_name in candidate:
            return month_number, month_name
    return None, None


def build_monthly_agrarian_curated(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return pl.DataFrame(
            schema={
                "categoria_agraria": pl.Utf8,
                "dominio_fuente": pl.Utf8,
                "mes_publicacion": pl.Int64,
                "mes_publicacion_nombre": pl.Utf8,
                "es_hoja_indice": pl.Boolean,
                "hoja_nombre": pl.Utf8,
                "fila_idx": pl.Int64,
                "columna_idx": pl.Int64,
                "columna_nombre": pl.Utf8,
                "celda_valor": pl.Utf8,
                "archivo_origen": pl.Utf8,
                "archivo_miembro": pl.Utf8,
                "tipo_archivo_origen": pl.Utf8,
                "archivo_anio_publicacion": pl.Int32,
                "archivo_fecha_descarga": pl.Utf8,
                "anio_publicacion": pl.Utf8,
                "archivo_hash": pl.Utf8,
                "archivo_tamano_bytes": pl.Int64,
                "archivo_firma_remota": pl.Utf8,
                "ruta_raw_origen": pl.Utf8,
                "registro_hash_fuente": pl.Utf8,
            }
        )

    candidate = pl.concat_str(
        [
            pl.col("archivo_miembro").fill_null(""),
            pl.lit(" "),
            pl.col("archivo_origen").fill_null(""),
        ],
        separator="",
    ).str.to_lowercase()

    category_expr = (
        pl.when(candidate.str.contains("agricola"))
        .then(pl.lit("produccion_agricola"))
        .when(candidate.str.contains("pecuario|avicola"))
        .then(pl.lit("produccion_pecuaria_avicola"))
        .when(candidate.str.contains("agroindustria"))
        .then(pl.lit("agroindustria"))
        .when(candidate.str.contains("comercio\\s*-?\\s*interno"))
        .then(pl.lit("comercio_interno"))
        .when(candidate.str.contains("comercio\\s*-?\\s*externo"))
        .then(pl.lit("comercio_exterior"))
        .when(candidate.str.contains("insumos\\s*y\\s*servicios|insumos-y-servicios"))
        .then(pl.lit("insumos_y_servicios_agrarios"))
        .otherwise(pl.lit("otros_agrarios"))
        .alias("categoria_agraria")
    )

    month_name_expr = (
        pl.when(candidate.str.contains("enero"))
        .then(pl.lit("enero"))
        .when(candidate.str.contains("febrero"))
        .then(pl.lit("febrero"))
        .when(candidate.str.contains("marzo"))
        .then(pl.lit("marzo"))
        .when(candidate.str.contains("abril"))
        .then(pl.lit("abril"))
        .when(candidate.str.contains("mayo"))
        .then(pl.lit("mayo"))
        .when(candidate.str.contains("junio"))
        .then(pl.lit("junio"))
        .when(candidate.str.contains("julio"))
        .then(pl.lit("julio"))
        .when(candidate.str.contains("agosto"))
        .then(pl.lit("agosto"))
        .when(candidate.str.contains("septiembre|setiembre"))
        .then(pl.lit("septiembre"))
        .when(candidate.str.contains("octubre"))
        .then(pl.lit("octubre"))
        .when(candidate.str.contains("noviembre"))
        .then(pl.lit("noviembre"))
        .when(candidate.str.contains("diciembre"))
        .then(pl.lit("diciembre"))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
        .alias("mes_publicacion_nombre")
    )

    month_number_expr = (
        pl.when(candidate.str.contains("enero"))
        .then(pl.lit(1))
        .when(candidate.str.contains("febrero"))
        .then(pl.lit(2))
        .when(candidate.str.contains("marzo"))
        .then(pl.lit(3))
        .when(candidate.str.contains("abril"))
        .then(pl.lit(4))
        .when(candidate.str.contains("mayo"))
        .then(pl.lit(5))
        .when(candidate.str.contains("junio"))
        .then(pl.lit(6))
        .when(candidate.str.contains("julio"))
        .then(pl.lit(7))
        .when(candidate.str.contains("agosto"))
        .then(pl.lit(8))
        .when(candidate.str.contains("septiembre|setiembre"))
        .then(pl.lit(9))
        .when(candidate.str.contains("octubre"))
        .then(pl.lit(10))
        .when(candidate.str.contains("noviembre"))
        .then(pl.lit(11))
        .when(candidate.str.contains("diciembre"))
        .then(pl.lit(12))
        .otherwise(pl.lit(None, dtype=pl.Int64))
        .alias("mes_publicacion")
    )

    return (
        df.with_columns(
            category_expr,
            pl.lit("agrario").alias("dominio_fuente"),
            month_number_expr,
            month_name_expr,
        )
        .with_columns(
            pl.col("hoja_nombre").str.to_uppercase().str.contains("INDICE").alias("es_hoja_indice")
        )
        .filter(pl.col("dominio_fuente") == "agrario")
        .select(
            [
                "categoria_agraria",
                "dominio_fuente",
                "mes_publicacion",
                "mes_publicacion_nombre",
                "es_hoja_indice",
                "hoja_nombre",
                "fila_idx",
                "columna_idx",
                "columna_nombre",
                "celda_valor",
                "archivo_origen",
                "archivo_miembro",
                "tipo_archivo_origen",
                "archivo_anio_publicacion",
                "archivo_fecha_descarga",
                "anio_publicacion",
                "archivo_hash",
                "archivo_tamano_bytes",
                "archivo_firma_remota",
                "ruta_raw_origen",
                "registro_hash_fuente",
            ]
        )
    )
