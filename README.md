# MySQL to PostgreSQL

[![PyPI](https://img.shields.io/pypi/v/mysql2pg)](https://pypi.org/project/mysql2pg/)
[![Publish](https://github.com/hello-seer/mysql2pg/actions/workflows/publish.yml/badge.svg)](https://github.com/hello-seer/mysql2pg/actions/workflows/publish.yml)

Copy rows from MySQL to PostgreSQL quickly.

## Example

<details>
<summary>Example</summary>

```sh
export PGHOST=localhost
export PGUSER=postgres
export MYSQL_DATABASE=example
export MYSQL_HOST=127.0.0.1
export MYSQL_USER=root

docker run -d -e MYSQL_ALLOW_EMPTY_PASSWORD=true -e MYSQL_DATABASE -p 3306:3306 --name mysql --rm mysql
mysql -D "$MYSQL_DATABASE" -u "$MYSQL_USER" <<EOF
CREATE TABLE example (a int, b text);
INSERT INTO example (a, b) VALUES (1, 'a'), (2, 'b'), (3, 'c');
EOF

docker run -d -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 --name postgres --rm postgres
psql <<EOF
CREATE TABLE example (a int, b text);
EOF

mysql2pg

psql <<EOF
TABLE example;
EOF

docker stop mysql
docker stop postgres
```

```txt
INFO      Found 1 tables
INFO      Truncating 1 tables
INFO      Copying table example
INFO      Copied 3 rows to table example (0.00s)

 a | b
---+---
 1 | a
 2 | b
 3 | c
(3 rows)
```

</details>

## Install

```sh
pip3 install mysql2pg
```

## Documentation

Mysql2pg copies records from MySQL tables to existing PostgreSQL tables.

Mysql2pg also resets sequences, based on the maximum column value.

Performance is limited by network bandwidth, database resources, record size,
and parallelism. In anecdotal usage, performance is 1m+ records/min serially.

### Usage

```txt
usage: mysql2pg [-h] [--log-level {critical,error,warning,info,debug}] [--parallelism PARALLELISM]
                [--pg-search-path PG_SEARCH_PATH]
                [tables ...]

Copy records from MySQL to PostgreSQL

positional arguments:
  tables

options:
  -h, --help            show this help message and exit
  --log-level {critical,error,warning,info,debug}
  --parallelism PARALLELISM
                        Number of tables to process in parallel (default: 10)
  --pg-search-path PG_SEARCH_PATH
                        PostgreSQL search path (default: None)
```

### Supported

- Parallelism
- Foreign keys
- PostgreSQL schema

### Not supported

- Creating PostgreSQL tables. If you need to convert schema, look at other tools
  like [mysql2postgres](https://github.com/maxlapshin/mysql2postgres).

## Implementation

1. MySQL tables are discovered.

2. PostgreSQL tables are truncated.

3. PostgreSQL triggers (including foreign keys) are disabled.

4. Records are streamed from MySQL and uploaded to PostgreSQL via the COPY
   command.

5. Sequences are discovered and reset based on the maximum value in the column.

Tables are processed in parallel.
