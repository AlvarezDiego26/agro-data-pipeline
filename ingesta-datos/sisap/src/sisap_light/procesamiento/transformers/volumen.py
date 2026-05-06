import polars as pl

from sisap_light.schemas import SisapQuery


EMPTY_MARKERS = {"", "...", "....", "-"}


def build_volumen_frame(rows: list[list[str]], query: SisapQuery | None = None) -> pl.DataFrame:
    if len(rows) < 2:
        return pl.DataFrame()

    header, *data = rows
    df = pl.DataFrame(data, schema=header, orient="row")
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

    melted = melted.with_columns(
        pl.col("producto_procedencia").str.split("__").list.get(0).alias("variedad"),
        pl.col("producto_procedencia").str.split("__").list.get(1).fill_null("Total").alias("procedencia"),
        pl.when(pl.col("volumen_ton").is_in(list(EMPTY_MARKERS)))
        .then(None)
        .otherwise(pl.col("volumen_ton"))
        .alias("volumen_ton"),
    ).drop("producto_procedencia")

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

