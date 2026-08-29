from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "sv"
    / "fact_deliveries"
)


# ---------------------------------------------------------------------------
# SPARK FIXTURE
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
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

    spark_session = configure_spark_with_delta_pip(
        builder
    ).getOrCreate()

    spark_session.sparkContext.setLogLevel("ERROR")

    yield spark_session

    spark_session.stop()


# ---------------------------------------------------------------------------
# DATAFRAME FIXTURE
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def silver_df(spark):
    if not SILVER_PATH.exists():
        pytest.fail(
            f"No existe la ruta Silver: {SILVER_PATH}"
        )

    return (
        spark.read
        .format("delta")
        .load(str(SILVER_PATH))
    )


# ---------------------------------------------------------------------------
# 1. DELTA STRUCTURE
# ---------------------------------------------------------------------------

def test_silver_delta_structure(spark):
    assert SILVER_PATH.exists(), (
        f"No existe la ruta Silver: {SILVER_PATH}"
    )

    detail = (
        spark.sql(
            f"DESCRIBE DETAIL delta.`{SILVER_PATH}`"
        )
        .select("format")
        .first()
    )

    assert detail is not None
    assert detail["format"].lower() == "delta"


# ---------------------------------------------------------------------------
# 2. DELTA READ
# ---------------------------------------------------------------------------

def test_silver_can_be_read_as_delta(silver_df):
    assert silver_df is not None


# ---------------------------------------------------------------------------
# 3. DATA EXISTS
# ---------------------------------------------------------------------------

def test_silver_contains_data(silver_df):
    assert silver_df.limit(1).count() > 0


# ---------------------------------------------------------------------------
# 4. TENANT COLUMN
# ---------------------------------------------------------------------------

def test_silver_contains_tenant_column(silver_df):
    assert "_tenant_id" in silver_df.columns


# ---------------------------------------------------------------------------
# 5. EXPECTED TENANT
# ---------------------------------------------------------------------------

def test_silver_contains_only_expected_tenant(silver_df):
    tenants = {
        row["_tenant_id"]
        for row in (
            silver_df
            .select("_tenant_id")
            .distinct()
            .collect()
        )
    }

    assert tenants == {"sv"}


# ---------------------------------------------------------------------------
# 6. NO DUPLICATE COLUMNS
# ---------------------------------------------------------------------------

def test_silver_has_no_duplicate_columns(silver_df):
    columns = silver_df.columns

    assert len(columns) == len(set(columns))


# ---------------------------------------------------------------------------
# 7. REQUIRED COLUMNS
# ---------------------------------------------------------------------------

def test_silver_contains_required_columns(silver_df):
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
        "_fecha_proceso_date",
        "is_routine_delivery",
        "is_bonus_delivery",
        "descripcion",
        "categoria",
        "precio_base",
        "cantidad_normalizada_st",
        "precio_transaccion",
    }

    missing = required_columns - set(silver_df.columns)

    assert not missing, (
        f"Faltan columnas requeridas en Silver: "
        f"{sorted(missing)}"
    )


# ---------------------------------------------------------------------------
# 8. VALID DELIVERY TYPES
# ---------------------------------------------------------------------------

def test_silver_delivery_types_are_valid(silver_df):
    valid_types = {
        "ZPRE",
        "ZVE1",
        "Z04",
        "Z05",
    }

    invalid_count = (
        silver_df
        .filter(
            ~F.col("tipo_entrega").isin(valid_types)
        )
        .count()
    )

    assert invalid_count == 0


# ---------------------------------------------------------------------------
# 9. UNIT NORMALIZATION
# ---------------------------------------------------------------------------

def test_silver_units_are_normalized_to_st(silver_df):
    invalid_units = (
        silver_df
        .filter(
            ~F.upper(
                F.trim(F.col("unidad"))
            ).isin("CS", "ST")
        )
        .count()
    )

    assert invalid_units == 0


# ---------------------------------------------------------------------------
# 10. NORMALIZED QUANTITY EXISTS
# ---------------------------------------------------------------------------

def test_silver_contains_normalized_quantity(silver_df):
    assert "cantidad_normalizada_st" in silver_df.columns


# ---------------------------------------------------------------------------
# 11. NORMALIZED QUANTITY IS NOT NULL
# ---------------------------------------------------------------------------

def test_silver_normalized_quantity_is_not_null(
    silver_df,
):
    null_count = (
        silver_df
        .filter(
            F.col("cantidad_normalizada_st").isNull()
        )
        .count()
    )

    assert null_count == 0


# ---------------------------------------------------------------------------
# 12. NORMALIZED QUANTITY IS POSITIVE
# ---------------------------------------------------------------------------

def test_silver_normalized_quantity_is_positive(
    silver_df,
):
    invalid_count = (
        silver_df
        .filter(
            F.col("cantidad_normalizada_st") <= 0
        )
        .count()
    )

    assert invalid_count == 0


# ---------------------------------------------------------------------------
# 13. CS -> ST CONVERSION
# ---------------------------------------------------------------------------

def test_silver_cs_quantity_is_converted_to_st(
    silver_df,
):
    invalid_conversion_count = (
        silver_df
        .filter(
            F.upper(
                F.trim(F.col("unidad"))
            ) == "CS"
        )
        .filter(
            F.col("cantidad_normalizada_st")
            != F.col("cantidad") * F.lit(20)
        )
        .count()
    )

    assert invalid_conversion_count == 0


# ---------------------------------------------------------------------------
# 14. ST REMAINS ST
# ---------------------------------------------------------------------------

def test_silver_st_quantity_is_preserved(
    silver_df,
):
    invalid_conversion_count = (
        silver_df
        .filter(
            F.upper(
                F.trim(F.col("unidad"))
            ) == "ST"
        )
        .filter(
            F.col("cantidad_normalizada_st")
            != F.col("cantidad")
        )
        .count()
    )

    assert invalid_conversion_count == 0


# ---------------------------------------------------------------------------
# 15. TRANSACTION PRICE EXISTS
# ---------------------------------------------------------------------------

def test_silver_contains_transaction_price(silver_df):
    assert "precio_transaccion" in silver_df.columns


# ---------------------------------------------------------------------------
# 16. TRANSACTION PRICE IS NOT NULL
# ---------------------------------------------------------------------------

def test_silver_transaction_price_is_not_null(
    silver_df,
):
    null_count = (
        silver_df
        .filter(
            F.col("precio_transaccion").isNull()
        )
        .count()
    )

    assert null_count == 0


# ---------------------------------------------------------------------------
# 17. TRANSACTION PRICE MATCHES ORIGINAL PRICE
# ---------------------------------------------------------------------------

def test_silver_transaction_price_matches_price(
    silver_df,
):
    mismatch_count = (
        silver_df
        .filter(
            F.col("precio_transaccion")
            != F.col("precio")
        )
        .count()
    )

    assert mismatch_count == 0


# ---------------------------------------------------------------------------
# 18. DELIVERY FLAGS
# ---------------------------------------------------------------------------

def test_silver_contains_delivery_flags(silver_df):
    assert "is_routine_delivery" in silver_df.columns
    assert "is_bonus_delivery" in silver_df.columns


# ---------------------------------------------------------------------------
# 19. ROUTINE FLAG
# ---------------------------------------------------------------------------

def test_routine_delivery_flag_is_consistent(
    silver_df,
):
    invalid_count = (
        silver_df
        .filter(
            (
                F.col("tipo_entrega").isin(
                    "ZPRE",
                    "ZVE1",
                )
            )
            != F.col("is_routine_delivery")
        )
        .count()
    )

    assert invalid_count == 0


# ---------------------------------------------------------------------------
# 20. BONUS FLAG
# ---------------------------------------------------------------------------

def test_bonus_delivery_flag_is_consistent(
    silver_df,
):
    invalid_count = (
        silver_df
        .filter(
            (
                F.col("tipo_entrega").isin(
                    "Z04",
                    "Z05",
                )
            )
            != F.col("is_bonus_delivery")
        )
        .count()
    )

    assert invalid_count == 0


# ---------------------------------------------------------------------------
# 21. QUANTITY
# ---------------------------------------------------------------------------

def test_silver_has_valid_quantity(silver_df):
    invalid_count = (
        silver_df
        .filter(
            F.col("cantidad").isNull()
            | (F.col("cantidad") <= 0)
        )
        .count()
    )

    assert invalid_count == 0


# ---------------------------------------------------------------------------
# 22. PRICE
# ---------------------------------------------------------------------------

def test_silver_has_valid_price(silver_df):
    invalid_count = (
        silver_df
        .filter(
            F.col("precio").isNull()
            | (F.col("precio") <= 0)
        )
        .count()
    )

    assert invalid_count == 0


# ---------------------------------------------------------------------------
# 23. DATE
# ---------------------------------------------------------------------------

def test_silver_has_valid_date(silver_df):
    invalid_count = (
        silver_df
        .filter(
            F.col("_fecha_proceso_date").isNull()
        )
        .count()
    )

    assert invalid_count == 0


# ---------------------------------------------------------------------------
# 24. TENANT / COUNTRY CONSISTENCY
# ---------------------------------------------------------------------------

def test_silver_tenant_is_consistent_with_country(
    silver_df,
):
    invalid_count = (
        silver_df
        .filter(
            F.col("_tenant_id") != "sv"
        )
        .count()
    )

    assert invalid_count == 0


# ---------------------------------------------------------------------------
# 25. MATERIAL DESCRIPTION
# ---------------------------------------------------------------------------

def test_silver_contains_material_description(
    silver_df,
):
    assert "descripcion" in silver_df.columns

    assert (
        silver_df
        .filter(F.col("descripcion").isNull())
        .count()
        == 0
    )


# ---------------------------------------------------------------------------
# 26. MATERIAL CATEGORY
# ---------------------------------------------------------------------------

def test_silver_contains_material_category(
    silver_df,
):
    assert "categoria" in silver_df.columns

    assert (
        silver_df
        .filter(F.col("categoria").isNull())
        .count()
        == 0
    )


# ---------------------------------------------------------------------------
# 27. MATERIAL BASE PRICE
# ---------------------------------------------------------------------------

def test_silver_contains_material_base_price(
    silver_df,
):
    assert "precio_base" in silver_df.columns

    assert (
        silver_df
        .filter(F.col("precio_base").isNull())
        .count()
        == 0
    )


# ---------------------------------------------------------------------------
# 28. MATERIAL ENRICHMENT
# ---------------------------------------------------------------------------

def test_silver_materials_are_enriched(silver_df):
    missing_material_count = (
        silver_df
        .filter(
            F.col("descripcion").isNull()
            | F.col("categoria").isNull()
            | F.col("precio_base").isNull()
        )
        .count()
    )

    assert missing_material_count == 0


# ---------------------------------------------------------------------------
# 29. BATCH ID
# ---------------------------------------------------------------------------

def test_silver_has_batch_id(silver_df):
    assert "_batch_id" in silver_df.columns

    assert (
        silver_df
        .filter(F.col("_batch_id").isNull())
        .count()
        == 0
    )


# ---------------------------------------------------------------------------
# 30. INGESTION TIMESTAMP
# ---------------------------------------------------------------------------

def test_silver_has_ingestion_timestamp(silver_df):
    assert "_ingestion_timestamp" in silver_df.columns

    assert (
        silver_df
        .filter(
            F.col("_ingestion_timestamp").isNull()
        )
        .count()
        == 0
    )


# ---------------------------------------------------------------------------
# 31. BUSINESS KEY NOT NULL
# ---------------------------------------------------------------------------

def test_silver_has_no_null_business_key_columns(
    silver_df,
):
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
        current_condition = F.col(column).isNull()

        if condition is None:
            condition = current_condition
        else:
            condition = condition | current_condition

    invalid_count = (
        silver_df
        .filter(condition)
        .count()
    )

    assert invalid_count == 0


# ---------------------------------------------------------------------------
# 32. BUSINESS KEY UNIQUENESS
# ---------------------------------------------------------------------------

def test_silver_has_no_duplicate_business_keys(
    silver_df,
):
    business_key = [
        "_tenant_id",
        "fecha_proceso",
        "transporte",
        "ruta",
        "material",
        "tipo_entrega",
    ]

    duplicate_count = (
        silver_df
        .groupBy(*business_key)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    assert duplicate_count == 0