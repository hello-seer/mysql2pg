import argparse


def main():
    parser = argparse.ArgumentParser(
        description="Copy records from MySQL to PostgreSQL", prog="mysql2pg"
    )
    parser.add_argument(
        "--parallelism",
        default=10,
        help="Number of tables to process in parallel",
        type=int,
    )
    parser.add_argument("--pg-search-path", help="PostgreSQL search path")
    args = parser.parse_args()

    import asyncio
    import logging
    import sys

    from .migrate import migrate

    logging.basicConfig(
        format="%(levelname)-9s %(message)s",
        handlers=[logging.StreamHandler(sys.stderr)],
        level=logging.INFO,
    )

    asyncio.run(
        migrate(parallelism=args.parallelism, pg_search_path=args.pg_search_path)
    )
