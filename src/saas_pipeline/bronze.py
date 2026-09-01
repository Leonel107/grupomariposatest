from pathlib import Path
from uuid import uuid4

from delta import configure_spark_with_delta_pip
from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


SOURCE_FILE = "global_mobility_data_entrega_productos.csv"
TABLE_NAME = "deliveries"


def create_spark_session() -> SparkSession:
    """Create a local Spark session configured with Delta Lake."""
    builder = (
        SparkSession.builder
        .appName("saas-bronze")
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

    return configure_spark_with_delta_pip(builder).getOrCreate()


def read_source(
    spark: SparkSession,
    source_path: Path,
) -> DataFrame:
    """Read the raw CSV without applying business transformations."""
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(source_path))
    )


def add_technical_columns(
    df: DataFrame,
    batch_id: str,
) -> DataFrame:
    """Add Bronze technical metadata while preserving source columns."""
    source_file = SOURCE_FILE

    return (
        df.withColumn(
            "_ingestion_timestamp",
            F.current_timestamp(),
        )
        .withColumn(
            "_source_file",
            F.lit(source_file),
        )
        .withColumn(
            "_tenant_id",
            F.lower(F.col("pais")),
        )
        .withColumn(
            "_batch_id",
            F.lit(batch_id),
        )
    )


def build_bronze_path(
    bronze_root: str,
    tenant: str,
) -> Path:
    """Build the Bronze table path for a tenant."""
    return Path(bronze_root) / tenant / TABLE_NAME


def filter_date_range(
    df: DataFrame,
    start_date: str,
    end_date: str,
) -> DataFrame:
    """
    Filter records using fecha_proceso.

    The source field remains a string in YYYYMMDD format.
    """
    start_yyyymmdd = start_date.replace("-", "")
    end_yyyymmdd = end_date.replace("-", "")

    return df.filter(
        F.col("fecha_proceso").between(
            start_yyyymmdd,
            end_yyyymmdd,
        )
    )


def write_bronze(
    df: DataFrame,
    output_path: Path,
    start_date: str,
    end_date: str,
    tenant: str,
) -> None:
    """
    Write Bronze data to Delta using partition overwrite.

    Reprocessing the same tenant/date range replaces the
    corresponding partitions instead of appending duplicates.
    """
    start_yyyymmdd = start_date.replace("-", "")
    end_yyyymmdd = end_date.replace("-", "")

    replace_condition = (
        f"fecha_proceso >= '{start_yyyymmdd}' "
        f"AND fecha_proceso <= '{end_yyyymmdd}' "
        f"AND _tenant_id = '{tenant}'"
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_condition)
        .partitionBy("fecha_proceso", "_tenant_id")
        .save(str(output_path))
    )


def process_tenant(
    spark: SparkSession,
    config: DictConfig,
    tenant: str,
) -> None:
    """Process the deliveries dataset for one tenant."""
    raw_root = Path(config.paths.raw)
    bronze_root = config.paths.bronze

    source_path = raw_root / SOURCE_FILE

    if not source_path.exists():
        raise FileNotFoundError(
            f"Source file not found: {source_path}"
        )

    batch_id = str(uuid4())

    df = read_source(spark, source_path)

    df = df.filter(
        F.lower(F.col("pais")) == tenant.lower()
    )

    df = filter_date_range(
        df=df,
        start_date=config.execution.start_date,
        end_date=config.execution.end_date,
    )

    df = add_technical_columns(
        df=df,
        batch_id=batch_id,
    )

    output_path = build_bronze_path(
        bronze_root=bronze_root,
        tenant=tenant.lower(),
    )

    write_bronze(
        df=df,
        output_path=output_path,
        start_date=config.execution.start_date,
        end_date=config.execution.end_date,
        tenant=tenant.lower(),
    )

    print(
        f"Bronze completed | "
        f"tenant={tenant.lower()} | "
        f"batch_id={batch_id} | "
        f"output={output_path}"
    )


def run_bronze(config: DictConfig) -> None:
    """Run Bronze ingestion according to the provided configuration."""
    spark = create_spark_session()

    try:
        tenant = config.execution.tenant

        if tenant == "all":
            tenants = (
                spark.read
                .option("header", True)
                .csv(
                    str(
                        Path(config.paths.raw)
                        / SOURCE_FILE
                    )
                )
                .select(F.lower(F.col("pais")).alias("tenant"))
                .distinct()
                .orderBy("tenant")
                .collect()
            )

            for row in tenants:
                process_tenant(
                    spark=spark,
                    config=config,
                    tenant=row["tenant"],
                )

        else:
            process_tenant(
                spark=spark,
                config=config,
                tenant=tenant,
            )

    finally:
        spark.stop()
