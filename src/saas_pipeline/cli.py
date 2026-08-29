import argparse

from saas_pipeline.bronze import run_bronze
from saas_pipeline.config import load_config
from saas_pipeline.silver import run_silver
from saas_pipeline.gold import run_gold


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="SAAS Data Platform pipeline"
    )

    parser.add_argument(
        "--layer",
        choices=["bronze", "silver","gold"],
        required=True,
        help="Pipeline layer to execute.",
    )

    parser.add_argument(
        "--environment",
        choices=["dev", "qa", "main"],
        default="dev",
        help="Execution environment.",
    )

    parser.add_argument(
        "--tenant",
        default="all",
        help="Tenant code or 'all'.",
    )

    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date in YYYY-MM-DD format.",
    )

    parser.add_argument(
        "--end-date",
        required=True,
        help="End date in YYYY-MM-DD format.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_arguments()

    config = load_config(
        environment=args.environment,
        tenant=args.tenant,
    )

    config.execution.start_date = args.start_date
    config.execution.end_date = args.end_date

    if args.layer == "bronze":
        run_bronze(config)

    elif args.layer == "silver":
        run_silver(config)

    elif args.layer == "gold":
        run_gold(config)


if __name__ == "__main__":
    main()