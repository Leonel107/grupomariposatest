from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType

from pyspark.sql.functions import (
    col,
    concat_ws,
    sha2,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]

from pyspark.sql.types import (
    StringType,
    IntegerType,
    DoubleType,
)


EXPECTED_RAW_SCHEMA = {
    "pais": StringType,
    "fecha_proceso": StringType,
    "transporte": IntegerType,
    "ruta": IntegerType,
    "tipo_entrega": StringType,
    "material": StringType,
    "precio": DoubleType,
    "cantidad": DoubleType,
    "unidad": StringType,
}

BRONZE_PATH = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / "sv"
    / "deliveries"
)

EXPECTED_TECHNICAL_COLUMNS = {
    "_ingestion_timestamp",
    "_source_file",
    "_tenant_id",
    "_batch_id",
}

RAW_COLUMNS = [
    "pais",
    "fecha_proceso",
    "transporte",
    "ruta",
    "tipo_entrega",
    "material",
    "precio",
    "cantidad",
    "unidad",
]

BRONZE_TECHNICAL_COLUMNS = [
    "_ingestion_timestamp",
    "_source_file",
    "_tenant_id",
    "_batch_id",
]

EXPECTED_TECHNICAL_SCHEMA = {
    "_ingestion_timestamp": TimestampType,
    "_source_file": StringType,
    "_tenant_id": StringType,
    "_batch_id": StringType,
}


@pytest.fixture(scope="module")
def spark():
    builder = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-bronze")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    yield spark

    spark.stop()


def test_bronze_delta_structure():
    """
    Verifica que la salida Bronze exista
    y tenga estructura Delta.
    """

    assert BRONZE_PATH.exists(), (
        f"No existe la ruta Bronze: {BRONZE_PATH}"
    )

    delta_log = BRONZE_PATH / "_delta_log"

    assert delta_log.exists(), (
        f"No se encontró _delta_log. "
        f"La ruta no parece ser una tabla Delta: {BRONZE_PATH}"
    )


def test_bronze_can_be_read_as_delta(spark):
    """
    Verifica que Bronze pueda ser leída como tabla Delta.
    """

    df = (
        spark.read
        .format("delta")
        .load(str(BRONZE_PATH))
    )

    assert df is not None
    assert len(df.columns) > 0


def test_bronze_contains_data(spark):
    """
    Verifica que Bronze contenga registros.
    """

    df = (
        spark.read
        .format("delta")
        .load(str(BRONZE_PATH))
    )

    assert df.limit(1).count() == 1


def test_bronze_contains_technical_columns(spark):
    """
    Verifica que Bronze conserve las columnas técnicas
    requeridas.
    """

    df = (
        spark.read
        .format("delta")
        .load(str(BRONZE_PATH))
    )

    columns = set(df.columns)

    missing_columns = EXPECTED_TECHNICAL_COLUMNS - columns

    assert not missing_columns, (
        f"Faltan columnas técnicas en Bronze: {missing_columns}"
    )


def test_bronze_contains_tenant_column(spark):
    """
    Verifica que Bronze contenga la columna técnica
    utilizada para identificar el tenant.
    """

    df = (
        spark.read
        .format("delta")
        .load(str(BRONZE_PATH))
    )

    assert "_tenant_id" in df.columns


def test_bronze_contains_only_expected_tenant(spark):
    """
    Verifica que la ejecución para SV solamente
    contenga registros correspondientes al tenant SV.
    """

    df = (
        spark.read
        .format("delta")
        .load(str(BRONZE_PATH))
    )

    tenants = {
        row["_tenant_id"]
        for row in (
            df
            .select("_tenant_id")
            .distinct()
            .collect()
        )
    }

    assert tenants == {"sv"}, (
        f"Se encontraron tenants inesperados en Bronze: {tenants}"
    )


def test_bronze_has_partition_columns(spark):
    """
    Verifica que Delta utilice fecha_proceso y _tenant_id
    como columnas de particionamiento.
    """

    detail = spark.sql(
        f"DESCRIBE DETAIL delta.`{BRONZE_PATH}`"
    )

    partition_columns = (
        detail
        .select("partitionColumns")
        .first()["partitionColumns"]
    )

    assert partition_columns == [
        "fecha_proceso",
        "_tenant_id",
    ], (
        "Particionamiento incorrecto. "
        f"Esperado=['fecha_proceso', '_tenant_id'], "
        f"obtenido={partition_columns}"
    )

def get_spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test-bronze")
        .getOrCreate()
    )


def get_bronze_path():
    return Path("data/bronze/sv/deliveries")


def test_bronze_contains_all_raw_columns():
    spark = get_spark()

    try:
        df_bronze = (
            spark.read
            .format("delta")
            .load(str(get_bronze_path()))
        )

        bronze_columns = set(df_bronze.columns)

        missing_columns = [
            column
            for column in RAW_COLUMNS
            if column not in bronze_columns
        ]

        assert not missing_columns, (
            f"Bronze perdió columnas originales de RAW: {missing_columns}"
        )

    finally:
        spark.stop()

def test_bronze_contains_only_raw_and_technical_columns():
    spark = get_spark()

    try:
        df_bronze = (
            spark.read
            .format("delta")
            .load(str(get_bronze_path()))
        )

        expected_columns = set(
            RAW_COLUMNS + BRONZE_TECHNICAL_COLUMNS
        )

        bronze_columns = set(df_bronze.columns)

        unexpected_columns = bronze_columns - expected_columns

        assert not unexpected_columns, (
            f"Bronze contiene columnas no esperadas: "
            f"{sorted(unexpected_columns)}"
        )

    finally:
        spark.stop()

def test_bronze_has_no_duplicate_columns():
    spark = get_spark()

    try:
        df_bronze = (
            spark.read
            .format("delta")
            .load(str(get_bronze_path()))
        )

        columns = df_bronze.columns

        assert len(columns) == len(set(columns)), (
            "Bronze contiene columnas duplicadas."
        )

    finally:
        spark.stop()

def test_bronze_raw_columns_have_expected_types():
    spark = get_spark()

    try:
        df_bronze = (
            spark.read
            .format("delta")
            .load(str(get_bronze_path()))
        )

        schema = {
            field.name: type(field.dataType)
            for field in df_bronze.schema.fields
        }

        for column, expected_type in EXPECTED_RAW_SCHEMA.items():
            assert column in schema, (
                f"No existe la columna '{column}' en Bronze."
            )

            assert schema[column] == expected_type, (
                f"Tipo incorrecto para '{column}'. "
                f"Esperado={expected_type.__name__}, "
                f"actual={schema[column].__name__}"
            )

    finally:
        spark.stop()

def test_bronze_technical_columns_have_expected_types():
    spark = get_spark()

    try:
        df_bronze = (
            spark.read
            .format("delta")
            .load(str(get_bronze_path()))
        )

        schema = {
            field.name: type(field.dataType)
            for field in df_bronze.schema.fields
        }

        for column, expected_type in EXPECTED_TECHNICAL_SCHEMA.items():
            assert column in schema, (
                f"No existe la columna técnica '{column}'."
            )

            assert schema[column] == expected_type, (
                f"Tipo incorrecto para '{column}'. "
                f"Esperado={expected_type.__name__}, "
                f"actual={schema[column].__name__}"
            )

    finally:
        spark.stop()

def test_raw_to_bronze_row_count():
    spark = get_spark()

    try:
        raw_path = (
            "data/raw/"
            "global_mobility_data_entrega_productos.csv"
        )

        bronze_path = "data/bronze/sv/deliveries"

        tenant = "SV"
        start_date = "20250101"
        end_date = "20250630"

        df_raw = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(raw_path)
        )

        df_bronze = (
            spark.read
            .format("delta")
            .load(bronze_path)
        )

        df_raw_filtered = df_raw.filter(
            (df_raw["pais"] == tenant)
            & (df_raw["fecha_proceso"] >= start_date)
            & (df_raw["fecha_proceso"] <= end_date)
        )

        raw_count = df_raw_filtered.count()
        bronze_count = df_bronze.count()

        assert bronze_count == raw_count, (
            f"Cantidad de registros diferente. "
            f"RAW filtrado={raw_count}, "
            f"Bronze={bronze_count}, "
            f"tenant={tenant}, "
            f"rango={start_date}-{end_date}"
        )

    finally:
        spark.stop()

def test_raw_to_bronze_content_integrity():
    spark = get_spark()

    try:
        raw_path = (
            "data/raw/"
            "global_mobility_data_entrega_productos.csv"
        )

        bronze_path = "data/bronze/sv/deliveries"

        tenant = "SV"
        start_date = "20250101"
        end_date = "20250630"

        # ---------------------------------------------------------
        # 1. Leer RAW
        # ---------------------------------------------------------
        df_raw = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(raw_path)
        )

        # ---------------------------------------------------------
        # 2. Leer BRONZE
        # ---------------------------------------------------------
        df_bronze = (
            spark.read
            .format("delta")
            .load(bronze_path)
        )

        # ---------------------------------------------------------
        # 3. Aplicar los mismos filtros utilizados por Bronze
        # ---------------------------------------------------------
        df_raw_filtered = df_raw.filter(
            (col("pais") == tenant)
            & (col("fecha_proceso") >= start_date)
            & (col("fecha_proceso") <= end_date)
        )

        # ---------------------------------------------------------
        # 4. Obtener únicamente las columnas originales
        # ---------------------------------------------------------
        raw_columns = df_raw.columns

        df_raw_content = df_raw_filtered.select(raw_columns)

        df_bronze_content = df_bronze.select(raw_columns)

        # ---------------------------------------------------------
        # 5. Generar hash de cada registro
        # ---------------------------------------------------------
        df_raw_hash = (
            df_raw_content
            .select(
                sha2(
                    concat_ws(
                        "||",
                        *[
                            col(column).cast("string")
                            for column in raw_columns
                        ]
                    ),
                    256,
                ).alias("row_hash")
            )
        )

        df_bronze_hash = (
            df_bronze_content
            .select(
                sha2(
                    concat_ws(
                        "||",
                        *[
                            col(column).cast("string")
                            for column in raw_columns
                        ]
                    ),
                    256,
                ).alias("row_hash")
            )
        )

        # ---------------------------------------------------------
        # 6. Detectar registros presentes en RAW pero ausentes
        #    en BRONZE
        # ---------------------------------------------------------
        raw_not_in_bronze = (
            df_raw_hash
            .subtract(df_bronze_hash)
            .count()
        )

        # ---------------------------------------------------------
        # 7. Detectar registros presentes en BRONZE pero ausentes
        #    en RAW
        # ---------------------------------------------------------
        bronze_not_in_raw = (
            df_bronze_hash
            .subtract(df_raw_hash)
            .count()
        )

        # ---------------------------------------------------------
        # 8. Validar integridad bidireccional
        # ---------------------------------------------------------
        assert raw_not_in_bronze == 0, (
            "Existen registros de RAW que no se encuentran "
            f"en Bronze. Registros faltantes={raw_not_in_bronze}"
        )

        assert bronze_not_in_raw == 0, (
            "Existen registros en Bronze que no corresponden "
            f"a RAW. Registros adicionales={bronze_not_in_raw}"
        )

    finally:
        spark.stop()