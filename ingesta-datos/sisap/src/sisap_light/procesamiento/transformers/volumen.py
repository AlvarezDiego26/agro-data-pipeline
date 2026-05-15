import polars as pl

from sisap_light.schemas import SisapQuery


EMPTY_MARKERS = {"", "...", "....", "-"}


def build_volumen_frame(rows: list[list[str]], query: SisapQuery | None = None) -> pl.DataFrame:
    if len(rows) < 2:
        return pl.DataFrame()

    header, *data = rows
    df = pl.DataFrame(data, schema=header, orient="row")

    # Caso 1: Estructura Snapshot (Producto, Variedad, Volumen, Procedencia)
    if "Producto" in df.columns and "Variedad" in df.columns:
        mapping = {"Variedad": "variedad"}
        if "Provincia de Procedencia" in df.columns:
            mapping["Provincia de Procedencia"] = "procedencia"
        
        # Buscamos la columna de volumen (ej: 'Volumen (t)')
        vol_cols = [c for c in df.columns if "volumen" in c.lower()]
        if vol_cols:
            mapping[vol_cols[0]] = "volumen_ton"
        
        df = df.rename(mapping)
        melted = df
    else:
        # Caso 2: Estructura Pivoteada (Fecha, Producto1__Proc1, Producto1__Proc2...)
        first_column = df.columns[0]
        if first_column != "Fecha":
            df = df.rename({first_column: "Fecha"})

        value_columns = [col for col in df.columns if col != "Fecha"]
        if not value_columns:
            return pl.DataFrame()

        melted = df.unpivot(
            index="Fecha",
            on=value_columns,
            variable_name="producto_procedencia",
            value_name="volumen_ton",
        )

        # `split_exact` evita errores de indice cuando la columna viene solo como
        # variedad (ej. "Alcachofa") y no como "Variedad__Procedencia".
        parts = pl.col("producto_procedencia").str.split_exact("__", 1)
        melted = melted.with_columns(
            pl.when(pl.col("producto_procedencia").str.contains("__"))
            .then(parts.struct.field("field_0"))
            .otherwise(pl.col("producto_procedencia"))
            .alias('variedad'),
            pl.when(pl.col("producto_procedencia").str.contains("__"))
            .then(parts.struct.field("field_1").fill_null("Total"))
            .otherwise(pl.lit("Consolidado"))
            .alias('procedencia'),
        ).drop("producto_procedencia")

    # Limpieza y tipado
    melted = melted.with_columns(
        pl.when(pl.col("volumen_ton").is_in(list(EMPTY_MARKERS)))
        .then(None)
        .otherwise(pl.col("volumen_ton"))
        .alias("volumen_ton"),
    )

    melted = melted.with_columns(
        pl.col("Fecha").str.strptime(pl.Date, format="%d/%m/%Y", strict=False).alias("fecha"),
        pl.col("volumen_ton").cast(pl.Float64, strict=False),
    ).drop("Fecha")

    melted = melted.drop_nulls(subset=["volumen_ton", "fecha"])

    if query is not None:
        melted = melted.with_columns(
            pl.lit(query.producto_codigo).alias("producto_codigo"),
            pl.lit(query.producto_nombre).alias("producto_nombre"),
            pl.lit(query.procedencia_codigo).alias("procedencia_filtro_codigo"),
            pl.lit(query.procedencia_nombre).alias("procedencia_filtro_nombre"),
            pl.lit(query.mercado_codigo).alias("mercado_codigo"),
            pl.lit(query.mercado_nombre).alias("mercado_nombre"),
            pl.lit(query.fecha_inicio).alias("fecha_inicio_consulta"),
            pl.lit(query.fecha_fin).alias("fecha_fin_consulta"),
        )

    ordered = [
        col
        for col in [
            "fecha",
            "producto_codigo",
            "producto_nombre",
            "variedad",
            "procedencia",
            "volumen_ton",
            "procedencia_filtro_codigo",
            "procedencia_filtro_nombre",
            "mercado_codigo",
            "mercado_nombre",
            "fecha_inicio_consulta",
            "fecha_fin_consulta",
        ]
        if col in melted.columns
    ]
    return melted.select(ordered)


