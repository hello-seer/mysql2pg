import argparse


def main():
    parser = argparse.ArgumentParser(
        description="Copy records from MySQL to PostgreSQL",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        prog="mysql2pg",
    )
    parser.add_argument(
        "--log-level",
        choices=("critical", "error", "warning", "info", "debug"),
        default="info",
    )
    parser.add_argument(
        "--parallelism",
        default=10,
        help="Number of tables to process in parallel",
        type=int,
    )
    parser.add_argument("--pg-search-path", help="PostgreSQL search path")
    parser.add_argument("tables", nargs="*")
    args = parser.parse_args()

    import asyncio
    import logging
    import sys

    from .migrate import migrate

    logging.basicConfig(
        format="%(levelname)-9s %(message)s",
        handlers=[logging.StreamHandler(sys.stderr)],
        level=args.log_level.upper(),
    )

    asyncio.run(
        migrate(
            parallelism=args.parallelism,
            pg_search_path=args.pg_search_path,
            tables=args.tables or None,
        )
    )
