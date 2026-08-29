from __future__ import annotations

from pathlib import Path

import pytest

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# CONFIGURATION
# ============================================================

GOLD_PATH = Path(
    "data/gold/sv/daily_metrics_by_delivery_type"
)

SILVER_PATH = Path(
    "data/silver/sv/fact_deliveries"
)

TENANT = "sv"

START_DATE = "2025-01-01"
END_DATE = "2025-06-30"


# ============================================================
# SPARK FIXTURE
# ============================================================

@pytest.fixture(scope="module")
def spark():
    """
    Creates a local Spark session configured with Delta Lake.
    """

    builder = (
        SparkSession.builder
        .appName("test-gold")
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

    spark = (
        configure_spark_with_delta_pip(builder)
        .getOrCreate()
    )

    yield spark

    spark.stop()


# ============================================================
# HELPERS
# ============================================================

@pytest.fixture(scope="module")
def gold_df(spark):
    """
    Reads the Gold Delta table.
    """

    assert GOLD_PATH.exists(), (
        f"No existe la ruta Gold: {GOLD_PATH}"
    )

    return (
        spark.read
        .format("delta")
        .load(str(GOLD_PATH))
    )


@pytest.fixture(scope="module")
def silver_df(spark):
    """
    Reads the Silver Delta table used as the source
    for Gold validation.
    """

    assert SILVER_PATH.exists(), (
        f"No existe la ruta Silver: {SILVER_PATH}"
    )

    return (
        spark.read
        .format("delta")
        .load(str(SILVER_PATH))
        .filter(
            F.col("_tenant_id") == TENANT
        )
        .filter(
            F.col("_fecha_proceso_date").between(
                F.to_date(F.lit(START_DATE)),
                F.to_date(F.lit(END_DATE)),
            )
        )
    )


# ============================================================
# 1. GOLD EXISTS
# ============================================================

def test_gold_delta_structure(spark):
    """
    Gold debe existir y estar almacenado como Delta.
    """

    assert GOLD_PATH.exists(), (
        f"No existe la ruta Gold: {GOLD_PATH}"
    )

    df = (
        spark.read
        .format("delta")
        .load(str(GOLD_PATH))
    )

    assert df is not None
    assert len(df.columns) > 0


# ============================================================
# 2. GOLD CAN BE READ AS DELTA
# ============================================================

def test_gold_can_be_read_as_delta(gold_df):
    """
    Verifica que la tabla Gold pueda ser leída
    utilizando el formato Delta.
    """

    assert gold_df is not None


# ============================================================
# 3. GOLD CONTAINS DATA
# ============================================================

def test_gold_contains_data(gold_df):
    """
    Gold debe contener registros.
    """

    assert gold_df.limit(1).count() == 1


# ============================================================
# 4. TENANT COLUMN
# ============================================================

def test_gold_contains_tenant_column(gold_df):
    """
    Gold debe conservar _tenant_id.
    """

    assert "_tenant_id" in gold_df.columns


# ============================================================
# 5. ONLY EXPECTED TENANT
# ============================================================

def test_gold_contains_only_expected_tenant(gold_df):
    """
    Gold debe contener únicamente el tenant procesado.
    """

    tenants = {
        row["_tenant_id"]
        for row in (
            gold_df
            .select("_tenant_id")
            .distinct()
            .collect()
        )
    }

    assert tenants == {TENANT}


# ============================================================
# 6. GOLD CONTRACT
# ============================================================

def test_gold_contains_required_columns(gold_df):
    """
    Valida el contrato de salida de Gold.
    """

    expected_columns = {
        "_tenant_id",
        "fecha_proceso",
        "tipo_entrega",
        "total_units",
        "total_revenue",
        "active_routes",
        "active_transports",
    }

    assert set(gold_df.columns) == expected_columns


# ============================================================
# 7. NO DUPLICATE COLUMNS
# ============================================================

def test_gold_has_no_duplicate_columns(gold_df):
    """
    No deben existir columnas duplicadas.
    """

    assert len(gold_df.columns) == len(
        set(gold_df.columns)
    )


# ============================================================
# 8. GOLD GRANULARITY
# ============================================================

def test_gold_has_unique_grain(gold_df):
    """
    La granularidad debe ser:

        (_tenant_id, fecha_proceso, tipo_entrega)

    No deben existir dos filas para la misma combinación.
    """

    grain = [
        "_tenant_id",
        "fecha_proceso",
        "tipo_entrega",
    ]

    duplicates = (
        gold_df
        .groupBy(*grain)
        .count()
        .filter(F.col("count") > 1)
    )

    assert duplicates.count() == 0


# ============================================================
# 9. DATE RANGE
# ============================================================

def test_gold_dates_are_inside_requested_range(gold_df):
    """
    Todas las fechas Gold deben estar dentro del rango
    solicitado durante la ejecución.
    """

    invalid_dates = (
        gold_df
        .filter(
            (F.to_date("fecha_proceso") < F.to_date(F.lit(START_DATE)))
            |
            (F.to_date("fecha_proceso") > F.to_date(F.lit(END_DATE)))
        )
    )

    assert invalid_dates.count() == 0


# ============================================================
# 10. DELIVERY TYPE
# ============================================================

def test_gold_delivery_types_are_not_null(gold_df):
    """
    tipo_entrega forma parte de la granularidad,
    por lo que no puede ser NULL.
    """

    assert (
        gold_df
        .filter(F.col("tipo_entrega").isNull())
        .count()
        == 0
    )


# ============================================================
# 11. TOTAL UNITS
# ============================================================

def test_gold_total_units_matches_silver(
    gold_df,
    silver_df,
):
    """
    total_units debe ser:

        SUM(cantidad_normalizada_st)

    agrupado por:

        (_tenant_id, fecha_proceso, tipo_entrega)
    """

    expected = (
        silver_df
        .groupBy(
            "_tenant_id",
            "fecha_proceso",
            "tipo_entrega",
        )
        .agg(
            F.sum(
                "cantidad_normalizada_st"
            ).alias("expected_total_units")
        )
    )

    comparison = (
        gold_df.alias("g")
        .join(
            expected.alias("s"),
            on=[
                "_tenant_id",
                "fecha_proceso",
                "tipo_entrega",
            ],
            how="left",
        )
        .filter(
            ~F.coalesce(
                F.col("g.total_units") ==
                F.col("s.expected_total_units"),
                F.lit(False),
            )
        )
    )

    assert comparison.count() == 0


# ============================================================
# 12. TOTAL REVENUE
# ============================================================

def test_gold_total_revenue_matches_silver(
    gold_df,
    silver_df,
):
    """
    total_revenue debe utilizar:

        SUM(
            cantidad_normalizada_st
            * precio_transaccion
        )

    Nunca precio_base.
    """

    expected = (
        silver_df
        .groupBy(
            "_tenant_id",
            "fecha_proceso",
            "tipo_entrega",
        )
        .agg(
            F.sum(
                F.col("cantidad_normalizada_st")
                * F.col("precio_transaccion")
            ).alias(
                "expected_total_revenue"
            )
        )
    )

    comparison = (
        gold_df.alias("g")
        .join(
            expected.alias("s"),
            on=[
                "_tenant_id",
                "fecha_proceso",
                "tipo_entrega",
            ],
            how="left",
        )
        .filter(
            F.abs(
                F.col("g.total_revenue")
                - F.col("s.expected_total_revenue")
            ) > 0.000001
        )
    )

    assert comparison.count() == 0


# ============================================================
# 13. ACTIVE ROUTES
# ============================================================

def test_gold_active_routes_matches_silver(
    gold_df,
    silver_df,
):
    """
    active_routes debe ser:

        COUNT(DISTINCT ruta)
    """

    expected = (
        silver_df
        .groupBy(
            "_tenant_id",
            "fecha_proceso",
            "tipo_entrega",
        )
        .agg(
            F.countDistinct(
                "ruta"
            ).alias(
                "expected_active_routes"
            )
        )
    )

    comparison = (
        gold_df.alias("g")
        .join(
            expected.alias("s"),
            on=[
                "_tenant_id",
                "fecha_proceso",
                "tipo_entrega",
            ],
            how="left",
        )
        .filter(
            F.col("g.active_routes")
            != F.col("s.expected_active_routes")
        )
    )

    assert comparison.count() == 0


# ============================================================
# 14. ACTIVE TRANSPORTS
# ============================================================

def test_gold_active_transports_matches_silver(
    gold_df,
    silver_df,
):
    """
    active_transports debe ser:

        COUNT(DISTINCT transporte)
    """

    expected = (
        silver_df
        .groupBy(
            "_tenant_id",
            "fecha_proceso",
            "tipo_entrega",
        )
        .agg(
            F.countDistinct(
                "transporte"
            ).alias(
                "expected_active_transports"
            )
        )
    )

    comparison = (
        gold_df.alias("g")
        .join(
            expected.alias("s"),
            on=[
                "_tenant_id",
                "fecha_proceso",
                "tipo_entrega",
            ],
            how="left",
        )
        .filter(
            F.col("g.active_transports")
            != F.col("s.expected_active_transports")
        )
    )

    assert comparison.count() == 0


# ============================================================
# 15. TOTAL UNITS VALIDITY
# ============================================================

def test_gold_total_units_are_valid(gold_df):
    """
    total_units debe ser numérico y no negativo.
    """

    invalid = (
        gold_df
        .filter(
            F.col("total_units").isNull()
            | (F.col("total_units") < 0)
        )
    )

    assert invalid.count() == 0


# ============================================================
# 16. TOTAL REVENUE VALIDITY
# ============================================================

def test_gold_total_revenue_is_valid(gold_df):
    """
    total_revenue debe ser numérico y no negativo.
    """

    invalid = (
        gold_df
        .filter(
            F.col("total_revenue").isNull()
            | (F.col("total_revenue") < 0)
        )
    )

    assert invalid.count() == 0


# ============================================================
# 17. ACTIVE ROUTES VALIDITY
# ============================================================

def test_gold_active_routes_are_valid(gold_df):
    """
    active_routes debe ser un entero positivo.
    """

    invalid = (
        gold_df
        .filter(
            F.col("active_routes").isNull()
            | (F.col("active_routes") <= 0)
        )
    )

    assert invalid.count() == 0


# ============================================================
# 18. ACTIVE TRANSPORTS VALIDITY
# ============================================================

def test_gold_active_transports_are_valid(gold_df):
    """
    active_transports debe ser un entero positivo.
    """

    invalid = (
        gold_df
        .filter(
            F.col("active_transports").isNull()
            | (F.col("active_transports") <= 0)
        )
    )

    assert invalid.count() == 0


# ============================================================
# 19. GOLD DOES NOT USE BASE PRICE
# ============================================================

def test_gold_contract_does_not_expose_base_price(gold_df):
    """
    Gold no debe exponer precio_base.

    precio_base pertenece al catálogo Silver y no debe
    utilizarse como revenue transaccional.
    """

    assert "precio_base" not in gold_df.columns


# ============================================================
# 20. GOLD DOES NOT EXPOSE TRANSACTION DETAIL
# ============================================================

def test_gold_is_aggregated(gold_df):
    """
    Gold debe contener métricas agregadas y no columnas
    de detalle transaccional de Silver.
    """

    forbidden_columns = {
        "cantidad",
        "cantidad_normalizada_st",
        "precio",
        "precio_transaccion",
        "ruta",
        "transporte",
        "material",
        "descripcion",
        "categoria",
    }

    assert not (
        forbidden_columns.intersection(
            set(gold_df.columns)
        )
    )