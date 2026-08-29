from pathlib import Path
from delta.tables import DeltaTable

import pytest

from delta import configure_spark_with_delta_pip

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SILVER_PATH = Path("data/silver/sv/fact_deliveries")
TENANT = "sv"

VALID_DELIVERY_TYPES = {
    "ZPRE",
    "ZVE1",
    "Z04",
    "Z05",
}


def get_spark() -> SparkSession:
    """Create Spark session configured for Delta Lake."""
    builder = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-silver")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def get_silver_df(spark: SparkSession):
    """Read the Silver fact_deliveries Delta table."""
    return (
        spark.read
        .format("delta")
        .load(str(SILVER_PATH))
    )

def test_silver_delta_structure():
    spark = get_spark()

    try:
        assert SILVER_PATH.exists(), (
            f"No existe la ruta Silver: {SILVER_PATH}"
        )

        assert DeltaTable.isDeltaTable(
            spark,
            str(SILVER_PATH),
        ), (
            f"La ruta Silver no corresponde a una tabla Delta: "
            f"{SILVER_PATH}"
        )

    finally:
        spark.stop()

def test_silver_can_be_read_as_delta():
    spark = get_spark()

    try:
        df = get_silver_df(spark)

        assert df is not None

    finally:
        spark.stop()


def test_silver_contains_data():
    spark = get_spark()

    try:
        df = get_silver_df(spark)

        assert df.count() > 0, (
            "Silver no contiene registros"
        )

    finally:
        spark.stop()


def test_silver_contains_tenant_column():
    spark = get_spark()

    try:
        df = get_silver_df(spark)

        assert "_tenant_id" in df.columns, (
            "Silver no contiene la columna técnica _tenant_id"
        )

    finally:
        spark.stop()


def test_silver_contains_only_expected_tenant():
    spark = get_spark()

    try:
        df = get_silver_df(spark)

        tenants = {
            row["_tenant_id"]
            for row in (
                df.select("_tenant_id")
                .distinct()
                .collect()
            )
        }

        assert tenants == {TENANT}, (
            f"Tenants encontrados en Silver: {tenants}"
        )

    finally:
        spark.stop()


def test_silver_has_no_duplicate_columns():
    spark = get_spark()

    try:
        df = get_silver_df(spark)

        columns_lower = [
            column.lower()
            for column in df.columns
        ]

        assert len(columns_lower) == len(set(columns_lower)), (
            "Silver contiene columnas duplicadas"
        )

    finally:
        spark.stop()


def test_silver_contains_required_columns():
    spark = get_spark()

    try:
        df = get_silver_df(spark)

        required_columns = {
            "pais",
            "fecha_proceso",
            "transporte",
            "ruta",
            "tipo_entrega",
            "material",
            "precio",
            "cantidad",
            "unidad",
            "_ingestion_timestamp",
            "_source_file",
            "_tenant_id",
            "_batch_id",
            "is_routine_delivery",
            "is_bonus_delivery",
        }

        missing_columns = (
            required_columns
            - set(df.columns)
        )

        assert not missing_columns, (
            f"Faltan columnas requeridas en Silver: "
            f"{missing_columns}"
        )

    finally:
        spark.stop()


def test_silver_delivery_types_are_valid():
    spark = get_spark()

    try:
        df = get_silver_df(spark)

        invalid_count = (
            df.filter(
                ~F.col("tipo_entrega").isin(
                    list(VALID_DELIVERY_TYPES)
                )
            )
            .count()
        )

        assert invalid_count == 0, (
            f"Existen {invalid_count} registros "
            "con tipo_entrega fuera del alcance Silver"
        )

    finally:
        spark.stop()


def test_silver_units_are_normalized_to_st():
    spark = get_spark()

    try:
        df = get_silver_df(spark)

        invalid_count = (
            df.filter(
                F.col("unidad") != "ST"
            )
            .count()
        )

        assert invalid_count == 0, (
            f"Existen {invalid_count} registros "
            "cuya unidad no está normalizada a ST"
        )

    finally:
        spark.stop()


def test_silver_contains_delivery_flags():
    spark = get_spark()

    try:
        df = get_silver_df(spark)

        assert "is_routine_delivery" in df.columns
        assert "is_bonus_delivery" in df.columns

    finally:
        spark.stop()


def test_routine_delivery_flag_is_consistent():
    spark = get_spark()

    try:
        df = get_silver_df(spark)

        invalid_count = (
            df.filter(
                (
                    F.col("tipo_entrega").isin(
                        ["ZPRE", "ZVE1"]
                    )
                )
                != F.col("is_routine_delivery")
            )
            .count()
        )

        assert invalid_count == 0, (
            "is_routine_delivery no es consistente "
            "con tipo_entrega"
        )

    finally:
        spark.stop()


def test_bonus_delivery_flag_is_consistent():
    spark = get_spark()

    try:
        df = get_silver_df(spark)

        invalid_count = (
            df.filter(
                (
                    F.col("tipo_entrega").isin(
                        ["Z04", "Z05"]
                    )
                )
                != F.col("is_bonus_delivery")
            )
            .count()
        )

        assert invalid_count == 0, (
            "is_bonus_delivery no es consistente "
            "con tipo_entrega"
        )

    finally:
        spark.stop()


def test_silver_has_valid_quantity():
    spark = get_spark()

    try:
        df = get_silver_df(spark)

        invalid_count = (
            df.filter(
                F.col("cantidad").isNull()
                | (F.col("cantidad") <= 0)
            )
            .count()
        )

        assert invalid_count == 0, (
            f"Existen {invalid_count} registros "
            "con cantidad nula, cero o negativa"
        )

    finally:
        spark.stop()


def test_silver_has_valid_price():
    spark = get_spark()

    try:
        df = get_silver_df(spark)

        invalid_count = (
            df.filter(
                F.col("precio").isNull()
            )
            .count()
        )

        assert invalid_count == 0, (
            f"Existen {invalid_count} registros "
            "con precio nulo"
        )

    finally:
        spark.stop()


def test_silver_has_valid_date():
    spark = get_spark()

    try:
        df = get_silver_df(spark)

        invalid_count = (
            df.filter(
                F.col("fecha_proceso").isNull()
            )
            .count()
        )

        assert invalid_count == 0, (
            f"Existen {invalid_count} registros "
            "con fecha_proceso nula"
        )

    finally:
        spark.stop()


def test_silver_tenant_is_consistent_with_country():
    spark = get_spark()

    try:
        df = get_silver_df(spark)

        invalid_count = (
            df.filter(
                F.lower(F.col("pais"))
                != F.lower(F.col("_tenant_id"))
            )
            .count()
        )

        assert invalid_count == 0, (
            "Existen registros donde pais y "
            "_tenant_id son inconsistentes"
        )

    finally:
        spark.stop()


def test_silver_contains_material_description():
    spark = get_spark()

    try:
        df = get_silver_df(spark)

        assert "descripcion" in df.columns, (
            "Silver no contiene descripcion "
            "del catálogo de materiales"
        )

    finally:
        spark.stop()


def test_silver_contains_material_category():
    spark = get_spark()

    try:
        df = get_silver_df(spark)

        assert "categoria" in df.columns, (
            "Silver no contiene categoria "
            "del catálogo de materiales"
        )

    finally:
        spark.stop()


def test_silver_contains_material_base_price():
    spark = get_spark()

    try:
        df = get_silver_df(spark)

        assert "precio_base" in df.columns, (
            "Silver no contiene precio_base "
            "del catálogo de materiales"
        )

    finally:
        spark.stop()


def test_silver_materials_are_enriched():
    spark = get_spark()

    try:
        df = get_silver_df(spark)

        missing_material_count = (
            df.filter(
                F.col("material").isNull()
            )
            .count()
        )

        assert missing_material_count == 0, (
            "Existen registros Silver sin material"
        )

        missing_description_count = (
            df.filter(
                F.col("descripcion").isNull()
            )
            .count()
        )

        assert missing_description_count == 0, (
            "Existen materiales Silver sin "
            "descripcion proveniente del catálogo"
        )

    finally:
        spark.stop()


def test_silver_has_batch_id():
    spark = get_spark()

    try:
        df = get_silver_df(spark)

        assert "_batch_id" in df.columns

        null_batch_count = (
            df.filter(
                F.col("_batch_id").isNull()
            )
            .count()
        )

        assert null_batch_count == 0, (
            "Existen registros Silver sin _batch_id"
        )

    finally:
        spark.stop()


def test_silver_has_ingestion_timestamp():
    spark = get_spark()

    try:
        df = get_silver_df(spark)

        assert "_ingestion_timestamp" in df.columns

        null_timestamp_count = (
            df.filter(
                F.col("_ingestion_timestamp").isNull()
            )
            .count()
        )

        assert null_timestamp_count == 0, (
            "Existen registros Silver sin "
            "_ingestion_timestamp"
        )

    finally:
        spark.stop()


def test_silver_has_no_null_business_key_columns():
    spark = get_spark()

    try:
        df = get_silver_df(spark)

        business_key = [
            "_tenant_id",
            "fecha_proceso",
            "transporte",
            "ruta",
            "material",
            "tipo_entrega",
        ]

        condition = None

        for column in business_key:
            current = F.col(column).isNull()

            if condition is None:
                condition = current
            else:
                condition = condition | current

        invalid_count = (
            df.filter(condition)
            .count()
        )

        assert invalid_count == 0, (
            f"Existen {invalid_count} registros "
            "con columnas nulas en la clave de negocio"
        )

    finally:
        spark.stop()


def test_silver_has_no_duplicate_business_keys():
    spark = get_spark()

    try:
        df = get_silver_df(spark)

        key_columns = [
            "_tenant_id",
            "fecha_proceso",
            "transporte",
            "ruta",
            "material",
            "tipo_entrega",
        ]

        duplicate_count = (
            df.groupBy(*key_columns)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert duplicate_count == 0, (
            f"Existen {duplicate_count} claves de negocio "
            "duplicadas en fact_deliveries"
        )

    finally:
        spark.stop()