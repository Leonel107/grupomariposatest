from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass(frozen=True)
class ProcessingConfig:
    """
    Business rules and processing configuration.
    """

    allowed_delivery_types: tuple[str, ...] = ("ZPRE", "ZVE1")
    units_to_normalize: tuple[str, ...] = ("CS",)
    units_per_case: int = 20


def get_spark() -> SparkSession:
    """Create or retrieve the Spark session."""

    return (
        SparkSession.builder
        .appName("SaaS Data Processing")
        .getOrCreate()
    )


def validate_input_columns(
    df: DataFrame,
    required_columns: set[str],
) -> None:
    """
    Validate that all required columns exist in the input DataFrame.
    """

    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            "Missing required columns: "
            f"{sorted(missing_columns)}"
        )


def validate_country(country: str) -> str:
    """Validate and normalize the tenant/country identifier."""

    if not country or not country.strip():
        raise ValueError("country must not be empty.")

    return country.strip().upper()


def transform_data(
    df: DataFrame,
    country: str,
    config: ProcessingConfig,
) -> DataFrame:
    """
    Filter and transform delivery data using native Spark operations.
    """

    required_columns = {
        "pais",
        "fecha_proceso",
        "material",
        "tipo_entrega",
        "unidad",
        "cantidad",
        "precio",
    }

    validate_input_columns(df, required_columns)

    country = validate_country(country)

    filtered_df = df.filter(
        (F.col("pais") == country)
        & F.col("tipo_entrega").isin(
            *config.allowed_delivery_types
        )
    )

    normalized_quantity = F.when(
        F.col("unidad").isin(*config.units_to_normalize),
        F.col("cantidad") * F.lit(config.units_per_case),
    ).otherwise(
        F.col("cantidad")
    )

    return filtered_df.select(
        F.col("pais"),
        F.col("fecha_proceso").alias("fecha"),
        F.col("material"),
        normalized_quantity.alias("cantidad_st"),
        (
            normalized_quantity * F.col("precio")
        ).alias("total"),
    )


def write_output(
    df: DataFrame,
    output_path: str,
) -> None:
    """
    Write the transformed dataset using an idempotent strategy.
    """

    output = Path(output_path)

    if not output:
        raise ValueError("output_path must not be empty.")

    (
        df.write
        .mode("overwrite")
        .parquet(str(output))
    )


def process(
    spark: SparkSession,
    file_path: str,
    country: str,
    output_base_path: str,
    config: ProcessingConfig | None = None,
) -> DataFrame:
    """
    Execute the complete processing flow for one tenant/country.
    """

    if config is None:
        config = ProcessingConfig()

    input_path = Path(file_path)

    if not input_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_path}"
        )

    country = validate_country(country)

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(input_path))
    )

    result = transform_data(
        df=df,
        country=country,
        config=config,
    )

    output_path = Path(output_base_path) / country

    write_output(
        df=result,
        output_path=str(output_path),
    )

    return result


def main() -> None:
    """Application entry point."""

    spark = get_spark()

    try:
        result = process(
            spark=spark,
            file_path="data.csv",
            country="GT",
            output_base_path="data/output",
        )

        print(
            f"Processing completed successfully. "
            f"Output columns: {result.columns}"
        )

    except Exception as exc:
        print(f"Processing failed: {exc}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()