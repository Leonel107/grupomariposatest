from __future__ import annotations

from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


DAILY_METRICS_BY_DELIVERY_TYPE = (
    "daily_metrics_by_delivery_type"
)


# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session() -> SparkSession:
    """
    Create a local Spark session configured with Delta Lake.
    """

    builder = (
        SparkSession.builder
        .appName("saas-gold")
        .master("local[1]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    return (
        configure_spark_with_delta_pip(builder)
        .getOrCreate()
    )


# ============================================================
# PATHS
# ============================================================

def build_silver_path(
    silver_root: str | Path,
    tenant: str,
) -> str:
    """
    Construye la ruta de la tabla Silver fact_deliveries.
    """

    return str(
        Path(silver_root)
        / tenant
        / "fact_deliveries"
    )


def build_gold_path(
    gold_root: str | Path,
    tenant: str,
) -> str:
    """
    Construye la ruta de la tabla Gold
    daily_metrics_by_delivery_type.
    """

    return str(
        Path(gold_root)
        / tenant
        / DAILY_METRICS_BY_DELIVERY_TYPE
    )


# ============================================================
# READ SILVER
# ============================================================

def read_silver_fact_deliveries(
    spark: SparkSession,
    silver_path: str,
) -> DataFrame:
    """
    Lee fact_deliveries desde Silver.
    """

    return (
        spark.read
        .format("delta")
        .load(silver_path)
    )


# ============================================================
# VALIDATION
# ============================================================

def validate_silver_schema(
    df: DataFrame,
) -> None:
    """
    Valida que Silver tenga las columnas necesarias
    para construir Gold.
    """

    required_columns = {
        "_tenant_id",
        "fecha_proceso",
        "_fecha_proceso_date",
        "tipo_entrega",
        "cantidad_normalizada_st",
        "precio_transaccion",
        "ruta",
        "transporte",
    }

    missing_columns = (
        required_columns - set(df.columns)
    )

    if missing_columns:
        raise ValueError(
            "Silver no contiene las columnas requeridas "
            "para Gold: "
            f"{sorted(missing_columns)}"
        )


# ============================================================
# DATE FILTER
# ============================================================

def filter_date_range(
    df: DataFrame,
    start_date: str,
    end_date: str,
) -> DataFrame:
    """
    Filtra Silver utilizando _fecha_proceso_date.

    Las fechas recibidas por configuración tienen formato:

        YYYY-MM-DD
    """

    start = F.to_date(
        F.lit(start_date),
        "yyyy-MM-dd",
    )

    end = F.to_date(
        F.lit(end_date),
        "yyyy-MM-dd",
    )

    return df.filter(
        F.col("_fecha_proceso_date").between(
            start,
            end,
        )
    )


# ============================================================
# GOLD METRICS
# ============================================================

def build_daily_metrics(
    df: DataFrame,
) -> DataFrame:
    """
    Construye daily_metrics_by_delivery_type.

    Granularidad:

        (_tenant_id, fecha_proceso, tipo_entrega)

    Métricas:

        total_units:
            SUM(cantidad_normalizada_st)

        total_revenue:
            SUM(
                cantidad_normalizada_st
                * precio_transaccion
            )

        active_routes:
            COUNT(DISTINCT ruta)

        active_transports:
            COUNT(DISTINCT transporte)
    """

    return (
        df.groupBy(
            "_tenant_id",
            "fecha_proceso",
            "tipo_entrega",
        )
        .agg(
            F.sum(
                F.col("cantidad_normalizada_st")
            ).alias(
                "total_units"
            ),

            F.sum(
                F.col("cantidad_normalizada_st")
                * F.col("precio_transaccion")
            ).alias(
                "total_revenue"
            ),

            F.countDistinct(
                "ruta"
            ).alias(
                "active_routes"
            ),

            F.countDistinct(
                "transporte"
            ).alias(
                "active_transports"
            ),
        )
    )


# ============================================================
# GOLD SCHEMA
# ============================================================

def select_gold_columns(
    df: DataFrame,
) -> DataFrame:
    """
    Define explícitamente el contrato de salida de Gold.
    """

    return df.select(
        "_tenant_id",
        "fecha_proceso",
        "tipo_entrega",
        "total_units",
        "total_revenue",
        "active_routes",
        "active_transports",
    )


# ============================================================
# WRITE GOLD
# ============================================================

def write_gold(
    df: DataFrame,
    output_path: str,
    tenant: str,
    start_date: str,
    end_date: str,
) -> None:
    """
    Escribe Gold en Delta.

    Gold es una capa derivada, por lo que se
    recomputa para el rango solicitado.

    fecha_proceso en Silver/Gold está representada
    como string con formato YYYYMMDD.
    """

    start_date_compact = (
        start_date.replace("-", "")
    )

    end_date_compact = (
        end_date.replace("-", "")
    )

    replace_where = (
        f"_tenant_id = '{tenant}' "
        f"AND fecha_proceso >= "
        f"'{start_date_compact}' "
        f"AND fecha_proceso <= "
        f"'{end_date_compact}'"
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "replaceWhere",
            replace_where,
        )
        .save(output_path)
    )


# ============================================================
# PROCESS TENANT
# ============================================================

def process_tenant(
    spark: SparkSession,
    config,
    tenant: str,
) -> None:
    """
    Procesa un tenant desde Silver hacia Gold.
    """

    silver_path = build_silver_path(
        silver_root=config.paths.silver,
        tenant=tenant,
    )

    gold_path = build_gold_path(
        gold_root=config.paths.gold,
        tenant=tenant,
    )

    print(
        f"[GOLD] Procesando tenant={tenant}"
    )

    # --------------------------------------------------------
    # READ SILVER
    # --------------------------------------------------------

    df_silver = read_silver_fact_deliveries(
        spark=spark,
        silver_path=silver_path,
    )

    # --------------------------------------------------------
    # VALIDATE SCHEMA
    # --------------------------------------------------------

    validate_silver_schema(
        df_silver
    )

    # --------------------------------------------------------
    # FILTER DATE RANGE
    # --------------------------------------------------------

    df_silver = filter_date_range(
        df=df_silver,
        start_date=config.execution.start_date,
        end_date=config.execution.end_date,
    )

    # --------------------------------------------------------
    # BUILD METRICS
    # --------------------------------------------------------

    df_gold = build_daily_metrics(
        df=df_silver
    )

    # --------------------------------------------------------
    # SELECT GOLD CONTRACT
    # --------------------------------------------------------

    df_gold = select_gold_columns(
        df_gold
    )

    # --------------------------------------------------------
    # WRITE GOLD
    # --------------------------------------------------------

    write_gold(
        df=df_gold,
        output_path=gold_path,
        tenant=tenant,
        start_date=config.execution.start_date,
        end_date=config.execution.end_date,
    )

    # --------------------------------------------------------
    # METRICS
    # --------------------------------------------------------

    records_written = df_gold.count()

    print(
        f"[GOLD] Tenant {tenant} procesado correctamente. "
        f"Registros Gold: {records_written}"
    )


# ============================================================
# RUN GOLD
# ============================================================

def run_gold(
    config,
) -> None:
    """
    Ejecuta el pipeline Gold para uno o todos los tenants.
    """

    spark = create_spark_session()

    try:

        tenant = config.execution.tenant

        if tenant == "all":

            tenants = config.tenants

            for tenant_config in tenants:

                process_tenant(
                    spark=spark,
                    config=config,
                    tenant=tenant_config,
                )

        else:

            process_tenant(
                spark=spark,
                config=config,
                tenant=tenant,
            )

    finally:

        spark.stop()