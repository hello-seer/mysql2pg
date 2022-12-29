#!/usr/bin/env python3
import pathlib

import setuptools

version = {}
exec(pathlib.Path("mysql2pg/version.py").read_text(), version)

setuptools.setup(
    author="Seer",
    author_email="engineering-admin@helloseer.com",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
    ],
    description="Copy data from MySQL to PostgreSQL",
    entry_points={
        "console_scripts": [
            "mysql2pg=mysql2pg.main:main",
        ]
    },
    extras_require={
        "dev": ["black", "isort", "pytest", "pytest-asyncio", "pytype", "twine"]
    },
    long_description=pathlib.Path("README.md").read_text(),
    long_description_content_type="text/markdown",
    install_requires=["aiomysql", "asyncpg"],
    name="mysql2pg",
    project_urls={
        "Issues": "https://github.com/helloseer/mysql2pg",
    },
    python_requires=">=3.7.0",
    version=version["__version__"],
)
