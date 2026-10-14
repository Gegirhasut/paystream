#!/bin/sh
# Separate database for the test suite, so tests never touch the service data.
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
    CREATE DATABASE payments_test OWNER "$POSTGRES_USER";
SQL
