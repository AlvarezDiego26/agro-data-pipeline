from pathlib import Path
from loguru import logger
import polars as pl
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
from midagri_boletines.config import get_settings

DATASET_BUSINESS_KEYS: dict[str, list[str]] = {
    "base/base_gmml_diario": ["registro_hash_fuente"],
    "base/base_agro_en_cifras": ["registro_hash_fuente"],
    "curated/gmml_diario_agrario": ["registro_hash_fuente"],
    "curated/agro_en_cifras_agrario": ["registro_hash_fuente"],
}


def deduplicate_dataset(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
    """Elimina registros duplicados en memoria usando la clave de negocio."""
    keys = DATASET_BUSINESS_KEYS.get(dataset_name)
    if not keys:
        logger.warning(f"No se configuró clave de negocio para {dataset_name}. Retornando sin deduplicar.")
        return df

    initial_count = len(df)
    deduped = df.unique(subset=keys, keep="first")
    final_count = len(deduped)
    
    if initial_count != final_count:
        logger.info(f"Deduplicados {initial_count - final_count} registros en memoria para {dataset_name}.")
    
    return deduped


def save_delta_table(
    df: pl.DataFrame,
    dataset_name: str,
    partition_by: list[str] | None = None,
    overwrite: bool = False
) -> None:
    """
    Persiste un DataFrame a una tabla Delta Lake en MinIO o almacenamiento local.
    Realiza una operación MERGE basada en la clave de negocio o sobreescribe.
    """
    settings = get_settings()
    uri = settings.build_delta_uri(dataset_name)
    storage_options = settings.delta_storage_options
    keys = DATASET_BUSINESS_KEYS.get(dataset_name, ["registro_hash_fuente"])

    logger.info(f"Guardando {len(df)} registros en la tabla Delta: {uri} (Modo: {'Overwrite' if overwrite else 'Merge'})")

    if not settings.delta_enabled:
        # Fallback local sin Delta si estuviera deshabilitado (no es el caso habitual)
        out_path = Path(uri)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(out_path)
        return

    # Si es sobreescritura limpia
    if overwrite:
        write_deltalake(
            uri,
            df.to_arrow(),
            mode="overwrite",
            partition_by=partition_by,
            storage_options=storage_options
        )
        logger.success(f"Tabla Delta sobreescrita con éxito en: {uri}")
        return

    # Operación transaccional Merge
    try:
        # Intentar cargar la tabla existente para hacer el Merge
        dt = DeltaTable(uri, storage_options=storage_options)
        
        # Generar la condición del Merge basada en las llaves
        # ej: "target.registro_hash_fuente = source.registro_hash_fuente"
        merge_cond = " AND ".join([f"target.{k} = source.{k}" for k in keys])
        
        (
            dt.merge(
                source=df.to_arrow(),
                predicate=merge_cond,
                source_alias="source",
                target_alias="target"
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
        logger.success(f"Deduplicación transaccional y MERGE completado con éxito en: {uri}")
    
    except Exception as e:
        # Si la tabla no existe en la ruta de MinIO, la creamos por primera vez
        err_msg = str(e)
        if "not found" in err_msg.lower() or "no such file" in err_msg.lower() or "object not found" in err_msg.lower() or "no log files" in err_msg.lower():
            logger.info(f"La tabla Delta no existe. Creándola por primera vez en: {uri}")
            write_deltalake(
                uri,
                df.to_arrow(),
                mode="append",
                partition_by=partition_by,
                storage_options=storage_options
            )
            logger.success(f"Tabla Delta inicializada y creada en: {uri}")
        else:
            logger.error(f"Falla al ejecutar el Merge en Delta: {e}")
            raise e
