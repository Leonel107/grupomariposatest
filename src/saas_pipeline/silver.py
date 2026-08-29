from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from delta import configure_spark_with_delta_pip
from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable


SOURCE_FILE = "global_mobility_data_entrega_productos.csv"
MATERIALS_CATALOG_FILE = "materials_catalog.csv"

FACT_DELIVERIES = "fact_deliveries"
DIM_MATERIALS = "dim_materials"

VALID_DELIVERY_TYPES = ["ZPRE", "ZVE1", "Z04", "Z05"]


# ---------------------------------------------------------------------------
# SPARK
# ---------------------------------------------------------------------------

def create_spark_session() -> SparkSession:
    """Create a local Spark session configured with Delta Lake."""

    builder = (
        SparkSession.builder
        .appName("saas-silver")
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


# ---------------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------------

def build_silver_path(
    silver_root: str,
    tenant: str,
    table_name: str,
) -> Path:
    """Build the Silver table path for a tenant."""

    return (
        Path(silver_root)
        / tenant.lower()
        / table_name
    )


def build_quarantine_path(
    quarantine_root: str,
    layer: str,
    tenant: str,
    table_name: str,
) -> Path:
    """
    Build the quarantine table path.

    Architecture:
        <quarantine_root>/<layer>_quarantine/<tenant>/<table>
    """

    return (
        Path(quarantine_root)
        / f"{layer}_quarantine"
        / tenant.lower()
        / table_name
    )


# ---------------------------------------------------------------------------
# READ BRONZE
# ---------------------------------------------------------------------------

def read_bronze(
    spark: SparkSession,
    bronze_root: str,
    tenant: str,
) -> DataFrame:
    """Read the Bronze deliveries Delta table for a tenant."""

    bronze_path = (
        Path(bronze_root)
        / tenant.lower()
        / "deliveries"
    )

    if not bronze_path.exists():
        raise FileNotFoundError(
            f"Bronze table not found: {bronze_path}"
        )

    return (
        spark.read
        .format("delta")
        .load(str(bronze_path))
    )


# ---------------------------------------------------------------------------
# READ MATERIALS CATALOG
# ---------------------------------------------------------------------------

def read_materials_catalog(
    spark: SparkSession,
    raw_root: str,
) -> DataFrame:
    """Read the materials catalog."""

    catalog_path = (
        Path(raw_root)
        / MATERIALS_CATALOG_FILE
    )

    if not catalog_path.exists():
        raise FileNotFoundError(
            f"Materials catalog not found: {catalog_path}"
        )

    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(catalog_path))
    )


# ---------------------------------------------------------------------------
# DATE NORMALIZATION
# ---------------------------------------------------------------------------

def normalize_dates(df: DataFrame) -> DataFrame:
    """
    Convert fecha_proceso to a proper date.

    Invalid or null values become NULL and are later
    classified as quarantine records.
    """

    return df.withColumn(
        "_fecha_proceso_date",
        F.to_date(
            F.col("fecha_proceso").cast("string"),
            "yyyyMMdd",
        ),
    )


# ---------------------------------------------------------------------------
# ANOMALY CLASSIFICATION
# ---------------------------------------------------------------------------

def classify_anomalies(
    df: DataFrame,
    materials_catalog: DataFrame,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Classify Silver records according to the architecture.

    Returns:
        valid_df
        quarantine_df
        discarded_df
    """

    catalog_materials = (
        materials_catalog
        .select("material")
        .distinct()
        .withColumn("_catalog_match", F.lit(True))
    )

    # ---------------------------------------------------------
    # Catalog existence validation
    # ---------------------------------------------------------
    #
    # IMPORTANT:
    # Select only the original delivery columns plus
    # _catalog_match. This prevents the catalog join from
    # creating a second column named "material".
    #
    enriched = (
        df.alias("d")
        .join(
            catalog_materials.alias("c"),
            F.col("d.material") == F.col("c.material"),
            "left",
        )
        .select(
            "d.*",
            F.col("c._catalog_match"),
        )
    )

    # ---------------------------------------------------------
    # Quarantine conditions
    # ---------------------------------------------------------

    quarantine_condition = (
        F.col("_fecha_proceso_date").isNull()
        |
        F.col("cantidad").isNull()
        |
        (F.col("cantidad") <= 0)
        |
        F.col("precio").isNull()
        |
        F.col("_catalog_match").isNull()
    )

    # ---------------------------------------------------------
    # Quarantine
    # ---------------------------------------------------------

    quarantine_df = (
        enriched
        .filter(quarantine_condition)
        .withColumn(
            "_quarantine_reason",
            F.when(
                F.col("_fecha_proceso_date").isNull(),
                F.lit("fecha_proceso_nula_o_invalida"),
            )
            .when(
                F.col("cantidad").isNull()
                | (F.col("cantidad") <= 0),
                F.lit("cantidad_nula_negativa_o_cero"),
            )
            .when(
                F.col("_catalog_match").isNull(),
                F.lit("material_no_presente_en_catalogo"),
            )
            .when(
                F.col("precio").isNull(),
                F.lit("precio_nulo"),
            )
            .otherwise(
                F.lit("unknown")
            ),
        )
        .drop("_catalog_match")
    )

    # ---------------------------------------------------------
    # Discarded records
    # ---------------------------------------------------------

    discarded_df = (
        enriched
        .filter(
            F.col("tipo_entrega").isNull()
            | ~F.col("tipo_entrega").isin(
                VALID_DELIVERY_TYPES
            )
        )
        .filter(~quarantine_condition)
        .drop("_catalog_match")
    )

    # ---------------------------------------------------------
    # Valid records
    # ---------------------------------------------------------

    valid_df = (
        enriched
        .filter(~quarantine_condition)
        .filter(
            F.col("tipo_entrega").isin(
                VALID_DELIVERY_TYPES
            )
        )
        .drop("_catalog_match")
    )

    return (
        valid_df,
        quarantine_df,
        discarded_df,
    )

# ---------------------------------------------------------------------------
# DEDUPLICATION
# ---------------------------------------------------------------------------

def deduplicate_exact_records(
    df: DataFrame,
) -> DataFrame:
    """
    Remove exact duplicate records.

    All columns are considered for the deduplication.
    """

    return df.dropDuplicates()


# ---------------------------------------------------------------------------
# UNIT NORMALIZATION
# ---------------------------------------------------------------------------

def normalize_units(
    df: DataFrame,
) -> DataFrame:
    """
    Normalize quantities to ST.

    Business rule:
        1 CS = 20 ST
    """

    return (
        df.withColumn(
            "cantidad",
            F.when(
                F.upper(F.col("unidad")) == "CS",
                F.col("cantidad") * F.lit(20),
            )
            .otherwise(F.col("cantidad")),
        )
        .withColumn(
            "unidad",
            F.lit("ST"),
        )
    )


# ---------------------------------------------------------------------------
# DELIVERY FLAGS
# ---------------------------------------------------------------------------

def add_delivery_flags(
    df: DataFrame,
) -> DataFrame:
    """Add routine and bonus delivery flags."""

    return (
        df.withColumn(
            "is_routine_delivery",
            F.col("tipo_entrega").isin(
                ["ZPRE", "ZVE1"]
            ),
        )
        .withColumn(
            "is_bonus_delivery",
            F.col("tipo_entrega").isin(
                ["Z04", "Z05"]
            ),
        )
    )


# ---------------------------------------------------------------------------
# MATERIALS SCD TYPE 2
# ---------------------------------------------------------------------------

def prepare_materials_catalog(
    df: DataFrame,
) -> DataFrame:
    """
    Prepare materials catalog for SCD Type 2.

    Expected source columns:

        material
        descripcion
        categoria
        precio_base
        valid_from
        valid_to
        is_current
    """

    required_columns = {
        "material",
        "descripcion",
        "categoria",
        "precio_base",
        "valid_from",
        "valid_to",
        "is_current",
    }

    missing = required_columns - set(df.columns)

    if missing:
        raise ValueError(
            "Materials catalog is missing columns: "
            f"{sorted(missing)}"
        )

    return (
        df
        .withColumn(
            "valid_from",
            F.to_date(
                F.col("valid_from"),
                "yyyy-MM-dd",
            ),
        )
        .withColumn(
            "valid_to",
            F.to_date(
                F.col("valid_to"),
                "yyyy-MM-dd",
            ),
        )
        .withColumn(
            "is_current",
            F.col("is_current").cast("boolean"),
        )
        .select(
            "material",
            "descripcion",
            "categoria",
            "precio_base",
            "valid_from",
            "valid_to",
            "is_current",
        )
    )


def write_dim_materials(
    df: DataFrame,
    output_path: Path,
) -> None:
    """
    Create or replace the local Silver dim_materials Delta table.

    The catalog already contains SCD Type 2 version information.
    """

    if output_path.exists():
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(str(output_path))
        )
    else:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .save(str(output_path))
        )


# ---------------------------------------------------------------------------
# TEMPORAL ENRICHMENT
# ---------------------------------------------------------------------------

def enrich_with_materials(
    deliveries_df: DataFrame,
    materials_df: DataFrame,
) -> DataFrame:
    """
    Enrich deliveries using temporal SCD Type 2 join.

    The historical version of the material is selected based
    on fecha_proceso.

    Rule:
        fecha_proceso >= valid_from
        AND fecha_proceso <= valid_to
    """

    return (
        deliveries_df.alias("d")
        .join(
            materials_df.alias("m"),
            (
                (F.col("d.material") == F.col("m.material"))
                &
                (
                    F.col("d._fecha_proceso_date")
                    >= F.col("m.valid_from")
                )
                &
                (
                    F.col("d._fecha_proceso_date")
                    <= F.col("m.valid_to")
                )
            ),
            "left",
        )
        .select(
            F.col("d.*"),
            F.col("m.descripcion"),
            F.col("m.categoria"),
            F.col("m.precio_base"),
        )
    )


# ---------------------------------------------------------------------------
# FACT DELIVERIES
# ---------------------------------------------------------------------------

def write_fact_deliveries(
    spark: SparkSession,
    df: DataFrame,
    output_path: Path,
) -> None:
    """
    MERGE Silver fact_deliveries using the business key:

        tenant_id
        fecha_proceso
        transporte
        ruta
        material
        tipo_entrega
    """

    business_condition = """
        target._tenant_id = source._tenant_id
        AND target.fecha_proceso = source.fecha_proceso
        AND target.transporte = source.transporte
        AND target.ruta = source.ruta
        AND target.material = source.material
        AND target.tipo_entrega = source.tipo_entrega
    """

    if not output_path.exists():

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .save(str(output_path))
        )

        return

    delta_table = DeltaTable.forPath(
        spark,
        str(output_path),
    )

    (
        delta_table.alias("target")
        .merge(
            df.alias("source"),
            business_condition,
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


# ---------------------------------------------------------------------------
# QUARANTINE
# ---------------------------------------------------------------------------

def write_quarantine(
    df: DataFrame,
    output_path: Path,
) -> None:
    """
    Persist Silver quarantine records.

    Quarantine records are intentionally kept outside
    fact_deliveries.
    """

    if df.limit(1).count() == 0:
        return

    (
        df.write
        .format("delta")
        .mode("append")
        .save(str(output_path))
    )


# ---------------------------------------------------------------------------
# PROCESS TENANT
# ---------------------------------------------------------------------------

def process_tenant(
    spark: SparkSession,
    config: DictConfig,
    tenant: str,
) -> None:
    """Process one tenant through Silver."""

    tenant = tenant.lower()

    silver_root = config.paths.silver
    quarantine_root = config.paths.quarantine_root
    raw_root = config.paths.raw
    bronze_root = config.paths.bronze

    # ---------------------------------------------------------
    # Read Bronze
    # ---------------------------------------------------------

    df = read_bronze(
        spark=spark,
        bronze_root=bronze_root,
        tenant=tenant,
    )

    # ---------------------------------------------------------
    # Read catalog
    # ---------------------------------------------------------

    materials_catalog = read_materials_catalog(
        spark=spark,
        raw_root=raw_root,
    )

    materials_catalog = prepare_materials_catalog(
        materials_catalog
    )

    # ---------------------------------------------------------
    # SCD Type 2 dimension
    # ---------------------------------------------------------

    dim_materials_path = build_silver_path(
        silver_root=silver_root,
        tenant=tenant,
        table_name=DIM_MATERIALS,
    )

    write_dim_materials(
        df=materials_catalog,
        output_path=dim_materials_path,
    )

    # ---------------------------------------------------------
    # Normalize date
    # ---------------------------------------------------------

    df = normalize_dates(df)

    # ---------------------------------------------------------
    # IMPORTANT:
    # No date-range filter is applied here.
    #
    # Invalid dates must reach quarantine.
    # ---------------------------------------------------------

    # ---------------------------------------------------------
    # Classify anomalies
    # ---------------------------------------------------------

    (
        valid_df,
        quarantine_df,
        discarded_df,
    ) = classify_anomalies(
        df=df,
        materials_catalog=materials_catalog,
    )

    # ---------------------------------------------------------
    # Exact deduplication
    # ---------------------------------------------------------

    valid_df = deduplicate_exact_records(
        valid_df
    )

    # ---------------------------------------------------------
    # Normalize units
    # ---------------------------------------------------------

    valid_df = normalize_units(
        valid_df
    )

    # ---------------------------------------------------------
    # Delivery flags
    # ---------------------------------------------------------

    valid_df = add_delivery_flags(
        valid_df
    )

    # ---------------------------------------------------------
    # Temporal material enrichment
    # ---------------------------------------------------------

    valid_df = enrich_with_materials(
        deliveries_df=valid_df,
        materials_df=materials_catalog,
    )

    # ---------------------------------------------------------
    # Write quarantine
    # ---------------------------------------------------------

    quarantine_path = build_quarantine_path(
        quarantine_root=quarantine_root,
        layer="silver",
        tenant=tenant,
        table_name=FACT_DELIVERIES,
    )

    write_quarantine(
        df=quarantine_df,
        output_path=quarantine_path,
    )

    # ---------------------------------------------------------
    # Write fact_deliveries
    # ---------------------------------------------------------

    fact_path = build_silver_path(
        silver_root=silver_root,
        tenant=tenant,
        table_name=FACT_DELIVERIES,
    )

    write_fact_deliveries(
        spark=spark,
        df=valid_df,
        output_path=fact_path,
    )

    # ---------------------------------------------------------
    # Metrics
    # ---------------------------------------------------------

    valid_count = valid_df.count()
    quarantine_count = quarantine_df.count()
    discarded_count = discarded_df.count()

    print(
        f"Silver completed | "
        f"tenant={tenant} | "
        f"valid={valid_count} | "
        f"quarantine={quarantine_count} | "
        f"discarded={discarded_count} | "
        f"fact={fact_path}"
    )


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

def run_silver(
    config: DictConfig,
) -> None:
    """Run Silver according to the supplied configuration."""

    spark = create_spark_session()

    try:
        tenant = config.execution.tenant

        if tenant == "all":

            bronze_root = config.paths.bronze

            bronze_root_path = Path(bronze_root)

            if not bronze_root_path.exists():
                raise FileNotFoundError(
                    f"Bronze root not found: {bronze_root_path}"
                )

            tenants = sorted(
                path.name
                for path in bronze_root_path.iterdir()
                if path.is_dir()
            )

            failures = []

            for current_tenant in tenants:

                try:
                    process_tenant(
                        spark=spark,
                        config=config,
                        tenant=current_tenant,
                    )

                except Exception as exc:

                    failures.append(
                        (
                            current_tenant,
                            str(exc),
                        )
                    )

                    if config.execution.fail_fast:
                        raise

            if failures:
                print(
                    "Silver completed with tenant failures:"
                )

                for failed_tenant, error in failures:
                    print(
                        f"  tenant={failed_tenant} | "
                        f"error={error}"
                    )

        else:

            process_tenant(
                spark=spark,
                config=config,
                tenant=tenant,
            )

    finally:
        spark.stop()