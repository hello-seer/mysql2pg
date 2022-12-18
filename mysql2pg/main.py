import argparse


def main():
    parser = argparse.ArgumentParser(prog="mysql2pg")
    parser.add_argument("--parallelism", default=10, type=int)
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

    asyncio.run(migrate(parallelism=args.parallelism))
