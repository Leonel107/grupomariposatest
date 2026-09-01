from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# ============================================================
# QUALITY LOG SCHEMA
# ============================================================

QUALITY_LOG_COLUMNS = [
    "_run_id",
    "_batch_id",
    "tenant_id",
    "layer",
    "table_name",
    "check_name",
    "check_severity",
    "records_checked",
    "records_failed",
    "check_passed",
    "executed_at",
]


FACT_DELIVERIES = "fact_deliveries"


# ============================================================
# SPARK
# ============================================================

def create_spark_session() -> SparkSession:
    """Create a local Spark session configured with Delta."""

    builder = (
        SparkSession.builder
        .appName("saas-quality")
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

    return configure_spark_with_delta_pip(
        builder
    ).getOrCreate()


# ============================================================
# PATHS
# ============================================================

def build_silver_path(
    silver_root: str | Path,
    tenant: str,
) -> str:
    """Build Silver fact_deliveries path."""

    return str(
        Path(silver_root)
        / tenant
        / FACT_DELIVERIES
    )


# ============================================================
# RESULT HELPER
# ============================================================

def _result(
    check_name: str,
    tenant_id: str,
    severity: str,
    failed_records: int,
    total_records: int,
) -> dict:
    """
    Build a standardized quality-check result.

    The internal result contract intentionally matches
    the quality log schema:

        check_name
        tenant_id
        check_severity
        records_checked
        records_failed
        check_passed
    """

    check_passed = failed_records == 0

    return {
        "check_name": check_name,
        "tenant_id": tenant_id,
        "check_severity": severity,
        "records_checked": total_records,
        "records_failed": failed_records,
        "check_passed": check_passed,
    }


# ============================================================
# CHECK 1 - REQUIRED COLUMNS
# ============================================================

def check_required_columns(
    df: DataFrame,
    tenant_id: str,
) -> dict:
    """
    Validate that all required Silver columns are present.

    A missing required column is a critical failure.

    Since the validation cannot evaluate row-level data when
    the schema itself is incomplete, records_checked is based
    on the number of rows available in the dataframe.
    """

    required_columns = {
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
    }

    missing = required_columns - set(df.columns)

    total_records = df.count()

    if missing:
        failed_records = total_records

        # If the dataframe is empty, the schema is still invalid.
        # Use 1 to ensure the quality check fails.
        if failed_records == 0:
            failed_records = 1

        return _result(
            check_name="required_columns",
            tenant_id=tenant_id,
            severity="critical",
            failed_records=failed_records,
            total_records=total_records,
        )

    return _result(
        check_name="required_columns",
        tenant_id=tenant_id,
        severity="critical",
        failed_records=0,
        total_records=total_records,
    )


# ============================================================
# CHECK 2 - BUSINESS KEY
# ============================================================

def check_business_key_not_null(
    df: DataFrame,
    tenant_id: str,
) -> dict:
    """
    Validate that business-key columns do not contain NULL.
    """

    business_key = [
        "_tenant_id",
        "fecha_proceso",
        "material",
        "tipo_entrega",
        "ruta",
        "transporte",
    ]

    total_records = df.count()

    condition = None

    for column in business_key:

        current_condition = (
            F.col(column).isNull()
        )

        if condition is None:
            condition = current_condition
        else:
            condition = (
                condition
                | current_condition
            )

    failed_records = (
        df.filter(condition)
        .count()
    )

    return _result(
        check_name="business_key_not_null",
        tenant_id=tenant_id,
        severity="critical",
        failed_records=failed_records,
        total_records=total_records,
    )


# ============================================================
# CHECK 3 - NORMALIZED UNITS
# ============================================================

def check_normalized_units(
    df: DataFrame,
    tenant_id: str,
) -> dict:
    """
    Validate that Silver units are normalized to ST.
    """

    total_records = df.count()

    invalid_condition = (
        F.col("unidad").isNull()
        | F.col("cantidad_normalizada_st").isNull()
        | (
            (F.upper(F.trim(F.col("unidad"))) == "ST")
            & (
                F.col("cantidad_normalizada_st")
                != F.col("cantidad")
            )
        )
        | (
            (F.upper(F.trim(F.col("unidad"))) == "CS")
            & (
                F.col("cantidad_normalizada_st")
                != F.col("cantidad") * F.lit(20)
            )
        )
        | (
            ~F.upper(F.trim(F.col("unidad"))).isin("ST", "CS")
        )
    )

    failed_records = (
        df.filter(invalid_condition)
        .count()
    )

    return _result(
        check_name="normalized_units",
        tenant_id=tenant_id,
        severity="critical",
        failed_records=failed_records,
        total_records=total_records,
    )


# ============================================================
# CHECK 4 - DELIVERY TYPE
# ============================================================

def check_delivery_type(
    df: DataFrame,
    tenant_id: str,
) -> dict:
    """
    Validate allowed delivery types.
    """

    allowed_types = [
        "ZPRE",
        "ZVE1",
        "Z04",
        "Z05",
    ]

    total_records = df.count()

    failed_records = (
        df.filter(
            ~F.col("tipo_entrega").isin(
                allowed_types
            )
        )
        .count()
    )

    return _result(
        check_name="delivery_type_allowed",
        tenant_id=tenant_id,
        severity="warning",
        failed_records=failed_records,
        total_records=total_records,
    )


# ============================================================
# CHECK 5 - TENANT CONSISTENCY
# ============================================================

def check_tenant_consistency(
    df: DataFrame,
    tenant_id: str,
) -> dict:
    """
    Validate tenant isolation.
    """

    total_records = df.count()

    failed_records = (
        df.filter(
            F.col("_tenant_id")
            != F.lit(tenant_id)
        )
        .count()
    )

    return _result(
        check_name="tenant_consistency",
        tenant_id=tenant_id,
        severity="critical",
        failed_records=failed_records,
        total_records=total_records,
    )


# ============================================================
# RUN CHECKS
# ============================================================

def run_quality_checks(
    df: DataFrame,
    tenant_id: str,
) -> list[dict]:
    """
    Run all Silver quality checks.

    Required-columns validation is executed first. If the
    schema is invalid, subsequent validations are skipped
    because they may reference missing columns.
    """

    required_columns_result = check_required_columns(
        df,
        tenant_id,
    )

    if not required_columns_result["check_passed"]:
        return [
            required_columns_result
        ]

    return [
        required_columns_result,

        check_business_key_not_null(
            df,
            tenant_id,
        ),

        check_normalized_units(
            df,
            tenant_id,
        ),

        check_delivery_type(
            df,
            tenant_id,
        ),

        check_tenant_consistency(
            df,
            tenant_id,
        ),
    ]


# ============================================================
# QUALITY LOG DATAFRAME
# ============================================================

def results_to_dataframe(
    spark: SparkSession,
    results: list[dict],
    run_id: str,
    batch_id: str,
) -> DataFrame:
    """
    Convert quality results to the required quality log schema.
    """

    executed_at = datetime.now(
        timezone.utc
    )

    rows = []

    for result in results:

        rows.append(
            {
                "_run_id": run_id,

                "_batch_id": batch_id,

                "tenant_id": result[
                    "tenant_id"
                ],

                "layer": "silver",

                "table_name": FACT_DELIVERIES,

                "check_name": result[
                    "check_name"
                ],

                "check_severity": result[
                    "check_severity"
                ],

                "records_checked": result[
                    "records_checked"
                ],

                "records_failed": result[
                    "records_failed"
                ],

                "check_passed": result[
                    "check_passed"
                ],

                "executed_at": executed_at,
            }
        )

    return (
        spark
        .createDataFrame(rows)
        .select(
            *QUALITY_LOG_COLUMNS
        )
    )


# ============================================================
# WRITE QUALITY LOGS
# ============================================================

def write_quality_logs(
    df: DataFrame,
    output_path: str | Path,
) -> None:
    """
    Persist quality logs as Delta.
    """

    (
        df.write
        .format("delta")
        .mode("append")
        .save(str(output_path))
    )


# ============================================================
# QUALITY GATE
# ============================================================

def has_critical_failure(
    results: list[dict],
) -> bool:
    """
    Return True when a critical check failed.
    """

    return any(
        result["check_severity"] == "critical"
        and not result["check_passed"]
        for result in results
    )


def validate_quality_gate(
    results: list[dict],
    fail_on_critical: bool,
) -> None:
    """
    Abort pipeline when a critical check fails
    and fail_on_critical is enabled.
    """

    if (
        fail_on_critical
        and has_critical_failure(results)
    ):

        failed_checks = [
            result["check_name"]
            for result in results
            if (
                result["check_severity"] == "critical"
                and not result["check_passed"]
            )
        ]

        raise RuntimeError(
            "Quality gate failed. "
            "Critical checks failed: "
            f"{failed_checks}"
        )


# ============================================================
# PROCESS TENANT
# ============================================================

def process_quality_tenant(
    spark: SparkSession,
    config,
    tenant: str,
) -> None:
    """
    Run quality checks for one tenant.
    """

    silver_path = build_silver_path(
        silver_root=config.paths.silver,
        tenant=tenant,
    )

    print(
        f"[QUALITY] Procesando tenant={tenant}"
    )

    # --------------------------------------------------------
    # Read Silver
    # --------------------------------------------------------

    df = (
        spark.read
        .format("delta")
        .load(silver_path)
    )

    # --------------------------------------------------------
    # Filter by execution range
    # --------------------------------------------------------

    start_date = F.to_date(
        F.lit(
            config.execution.start_date
        ),
        "yyyy-MM-dd",
    )

    end_date = F.to_date(
        F.lit(
            config.execution.end_date
        ),
        "yyyy-MM-dd",
    )

    df = df.filter(
        F.col(
            "_fecha_proceso_date"
        ).between(
            start_date,
            end_date,
        )
    )

    # --------------------------------------------------------
    # Execute quality checks
    # --------------------------------------------------------

    results = run_quality_checks(
        df=df,
        tenant_id=tenant,
    )

    # --------------------------------------------------------
    # Obtain batch identifier from Silver
    # --------------------------------------------------------

    batch_row = (
        df.select("_batch_id")
        .where(
            F.col("_batch_id").isNotNull()
        )
        .limit(1)
        .collect()
    )

    batch_id = (
        batch_row[0]["_batch_id"]
        if batch_row
        else "unknown"
    )

    # --------------------------------------------------------
    # Generate run identifier
    # --------------------------------------------------------

    run_id = str(uuid4())

    # --------------------------------------------------------
    # Build quality log dataframe
    # --------------------------------------------------------

    quality_df = results_to_dataframe(
        spark=spark,
        results=results,
        run_id=run_id,
        batch_id=batch_id,
    )

    # --------------------------------------------------------
    # Persist quality logs
    # --------------------------------------------------------

    write_quality_logs(
        df=quality_df,
        output_path=config.paths.quality_logs,
    )

    # --------------------------------------------------------
    # Quality gate
    # --------------------------------------------------------

    validate_quality_gate(
        results=results,
        fail_on_critical=(
            config.quality.fail_on_critical
        ),
    )

    print(
        f"[QUALITY] Tenant {tenant} "
        "validado correctamente."
    )


# ============================================================
# MAIN ENTRYPOINT
# ============================================================

def run_quality(
    config,
) -> None:
    """
    Execute Silver quality validation.
    """

    spark = create_spark_session()

    try:

        tenant = config.execution.tenant

        if tenant == "all":

            tenants = config.tenants

            for tenant_config in tenants:

                process_quality_tenant(
                    spark=spark,
                    config=config,
                    tenant=tenant_config,
                )

        else:

            process_quality_tenant(
                spark=spark,
                config=config,
                tenant=tenant,
            )

    finally:

        spark.stop()
