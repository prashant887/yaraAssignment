export PGPASSWORD=postgres
USER=postgres
DB=postgres

psql -h localhost -d ${DB} -U ${USER} -p 5432 -a -q -f load_staging_tables.sql

