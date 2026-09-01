from __future__ import annotations

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from saas_pipeline.quality import (
    QUALITY_LOG_COLUMNS,
    check_business_key_not_null,
    check_delivery_type,
    check_normalized_units,
    check_required_columns,
    check_tenant_consistency,
    has_critical_failure,
    results_to_dataframe,
    validate_quality_gate,
)


TENANT = "sv"


# ============================================================================
# SPARK SESSION
# ============================================================================


def get_spark() -> SparkSession:
    """Create a local Spark session configured with Delta Lake."""

    builder = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-quality")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = configure_spark_with_delta_pip(
        builder
    ).getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    return spark


# ============================================================================
# TEST DATA
# ============================================================================


def create_valid_dataframe(
    spark: SparkSession,
):
    """
    Create a valid Silver-like DataFrame.

    Contains the columns required by the quality checks.
    """

    data = [
        (
            "sv",
            "2025-01-01",
            "ZPRE",
            "100001",
            10.0,
            "ST",
            10.0,
            5.0,
            5.0,
            "R001",
            "T001",
        ),
        (
            "sv",
            "2025-01-02",
            "ZVE1",
            "100002",
            40.0,
            "ST",
            40.0,
            3.0,
            3.0,
            "R002",
            "T002",
        ),
    ]

    columns = [
        "_tenant_id",
        "fecha_proceso",
        "tipo_entrega",
        "material",
        "cantidad",
        "unidad",
        "cantidad_normalizada_st",
        "precio",
        "precio_transaccion",
        "ruta",
        "transporte",
    ]

    return spark.createDataFrame(
        data,
        columns,
    )


# ============================================================================
# 1. REQUIRED COLUMNS
# ============================================================================


def test_required_columns_pass():
    spark = get_spark()

    try:
        df = create_valid_dataframe(spark)

        result = check_required_columns(
            df,
            TENANT,
        )

        assert result["check_passed"] is True
        assert result["check_severity"] == "critical"
        assert result["records_failed"] == 0
        assert result["records_checked"] == 2

    finally:
        spark.stop()


def test_required_columns_fail_when_missing():
    spark = get_spark()

    try:
        df = create_valid_dataframe(spark)

        df = df.drop(
            "precio_transaccion"
        )

        result = check_required_columns(
            df,
            TENANT,
        )

        assert result["check_passed"] is False
        assert result["check_severity"] == "critical"
        assert result["records_failed"] == 2
        assert result["records_checked"] == 2

    finally:
        spark.stop()


# ============================================================================
# 2. BUSINESS KEY
# ============================================================================


def test_business_key_not_null_pass():

    spark = get_spark()

    try:
        df = create_valid_dataframe(spark)

        result = check_business_key_not_null(
            df,
            TENANT,
        )

        assert result["check_passed"] is True
        assert result["check_severity"] == "critical"
        assert result["records_failed"] == 0
        assert result["records_checked"] == 2

    finally:
        spark.stop()


def test_business_key_not_null_fail():

    spark = get_spark()

    try:
        df = create_valid_dataframe(spark)

        # Introduce NULL into a business-key column.
        df = df.withColumn(
            "material",
            F.when(
                F.col("material") == "100001",
                F.lit(None),
            ).otherwise(
                F.col("material")
            ),
        )

        result = check_business_key_not_null(
            df,
            TENANT,
        )

        assert result["check_passed"] is False
        assert result["check_severity"] == "critical"
        assert result["records_failed"] == 1
        assert result["records_checked"] == 2

    finally:
        spark.stop()


# ============================================================================
# 3. NORMALIZED UNITS
# ============================================================================


def test_normalized_units_pass():

    spark = get_spark()

    try:
        df = create_valid_dataframe(spark)

        result = check_normalized_units(
            df,
            TENANT,
        )

        assert result["check_passed"] is True
        assert result["check_severity"] == "critical"
        assert result["records_failed"] == 0
        assert result["records_checked"] == 2

    finally:
        spark.stop()


def test_normalized_units_fail():

    spark = get_spark()

    try:
        df = create_valid_dataframe(spark)

        # Introduce a non-normalized unit.
        df = df.withColumn(
            "unidad",
            F.when(
                F.col("material") == "100001",
                F.lit("CS"),
            ).otherwise(
                F.col("unidad")
            ),
        )

        result = check_normalized_units(
            df,
            TENANT,
        )

        assert result["check_passed"] is False
        assert result["check_severity"] == "critical"
        assert result["records_failed"] == 1
        assert result["records_checked"] == 2

    finally:
        spark.stop()


# ============================================================================
# 4. DELIVERY TYPE
# ============================================================================


def test_delivery_type_pass():

    spark = get_spark()

    try:
        df = create_valid_dataframe(spark)

        result = check_delivery_type(
            df,
            TENANT,
        )

        assert result["check_passed"] is True
        assert result["check_severity"] == "warning"
        assert result["records_failed"] == 0
        assert result["records_checked"] == 2

    finally:
        spark.stop()


def test_delivery_type_fail():

    spark = get_spark()

    try:
        df = create_valid_dataframe(spark)

        # Introduce an invalid delivery type.
        df = df.withColumn(
            "tipo_entrega",
            F.when(
                F.col("material") == "100001",
                F.lit("INVALID"),
            ).otherwise(
                F.col("tipo_entrega")
            ),
        )

        result = check_delivery_type(
            df,
            TENANT,
        )

        assert result["check_passed"] is False
        assert result["check_severity"] == "warning"
        assert result["records_failed"] == 1
        assert result["records_checked"] == 2

    finally:
        spark.stop()


# ============================================================================
# 5. TENANT CONSISTENCY
# ============================================================================


def test_tenant_consistency_pass():

    spark = get_spark()

    try:
        df = create_valid_dataframe(spark)

        result = check_tenant_consistency(
            df,
            TENANT,
        )

        assert result["check_passed"] is True
        assert result["check_severity"] == "critical"
        assert result["records_failed"] == 0
        assert result["records_checked"] == 2

    finally:
        spark.stop()


def test_tenant_consistency_fail():

    spark = get_spark()

    try:
        df = create_valid_dataframe(spark)

        # Introduce a record belonging to another tenant.
        df = df.withColumn(
            "_tenant_id",
            F.when(
                F.col("material") == "100001",
                F.lit("other_tenant"),
            ).otherwise(
                F.col("_tenant_id")
            ),
        )

        result = check_tenant_consistency(
            df,
            TENANT,
        )

        assert result["check_passed"] is False
        assert result["check_severity"] == "critical"
        assert result["records_failed"] == 1
        assert result["records_checked"] == 2

    finally:
        spark.stop()


# ============================================================================
# 6. QUALITY GATE
# ============================================================================


def test_quality_gate_passes_when_no_critical_failure():

    results = [
        {
            "check_name": "required_columns",
            "check_severity": "critical",
            "check_passed": True,
        },
        {
            "check_name": "normalized_units",
            "check_severity": "critical",
            "check_passed": True,
        },
    ]

    assert (
        has_critical_failure(results)
        is False
    )

    validate_quality_gate(
        results,
        fail_on_critical=True,
    )


def test_quality_gate_fails_on_critical():

    results = [
        {
            "check_name": "required_columns",
            "check_severity": "critical",
            "check_passed": False,
        },
    ]

    assert (
        has_critical_failure(results)
        is True
    )

    try:
        validate_quality_gate(
            results,
            fail_on_critical=True,
        )

        assert False, (
            "Quality gate should fail "
            "when a critical check fails."
        )

    except RuntimeError as exc:
        assert "Quality gate failed" in str(exc)


def test_quality_gate_ignores_warning_failure():

    results = [
        {
            "check_name": "delivery_type_allowed",
            "check_severity": "warning",
            "check_passed": False,
        },
    ]

    assert (
        has_critical_failure(results)
        is False
    )

    validate_quality_gate(
        results,
        fail_on_critical=True,
    )


# ============================================================================
# 7. QUALITY LOG SCHEMA
# ============================================================================


def test_quality_log_schema():

    spark = get_spark()

    try:
        results = [
            {
                "check_name": "required_columns",
                "tenant_id": TENANT,
                "check_severity": "critical",
                "records_failed": 0,
                "records_checked": 2,
                "check_passed": True,
            }
        ]

        df = results_to_dataframe(
            spark=spark,
            results=results,
            run_id="test-run-001",
            batch_id="test-batch-001",
        )

        assert df is not None

        # Validate exact quality log schema.
        assert set(df.columns) == set(
            QUALITY_LOG_COLUMNS
        )

        assert df.count() == 1

        row = df.first()

        assert row["_run_id"] == "test-run-001"
        assert row["_batch_id"] == "test-batch-001"
        assert row["tenant_id"] == TENANT

        # Quality log is currently generated for Silver.
        assert row["layer"] == "silver"
        assert row["table_name"] == "fact_deliveries"

        assert row["check_name"] == "required_columns"
        assert row["check_severity"] == "critical"

        assert row["records_failed"] == 0
        assert row["records_checked"] == 2
        assert row["check_passed"] is True

        assert row["executed_at"] is not None

    finally:
        spark.stop()
