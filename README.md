# MySQL to PostgreSQL

Copy rows from MySQL to PostgreSQL quickly.

## Install

```sh
pip3 install mysql2pg
```

## Documentation

Mysql2pg copies records from MySQL tables to existing PostgreSQL tables.

Mysql2pg also resets sequences, based on the maximum column value.

Performance is limited by network bandwidth, database resources, record size,
and parallelism. In anecdotal usage, performance is 500k records/min serially.

### Supported

- Parallelism
- Foreign keys
- PostgreSQL schema

### Not supported

- Creating PostgreSQL tables. If you need to convert schema, look at other tools
  like [mysql2postgres](https://github.com/maxlapshin/mysql2postgres).
- Limiting the tables.

## Implementation

1. MySQL tables are discovered.

2. PostgreSQL tables are truncated.

3. PostgreSQL triggers (including foreign keys) are disabled.

4. Records are streamed from MySQL and uploaded to PostgreSQL via the COPY
   command.

5. Sequences are discovered and reset based on the maximum value in the column.

Tables are processed in parallel.

## Usage

```txt
usage: mysql2pg [-h] [--parallelism PARALLELISM] [--pg-search-path PG_SEARCH_PATH]

Copy records from MySQL to PostgreSQL

options:
  -h, --help            show this help message and exit
  --parallelism PARALLELISM
                        Number of tables to process in parallel
  --pg-search-path PG_SEARCH_PATH
                        PostgreSQL search path
```
