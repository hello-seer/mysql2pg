import argparse


def main():
    parser = argparse.ArgumentParser(prog="mysql2pg")
    parser.add_argument("--parallelism", default=10, type=int)
    args = parser.parse_args()

    import asyncio

    from .migrate import migrate

    asyncio.run(migrate())
