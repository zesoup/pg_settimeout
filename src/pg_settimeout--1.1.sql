/* contrib/worker_spi/worker_spi--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_settimeout" to load this file. \quit

CREATE FUNCTION pg_settimeout(pg_catalog.text, pg_catalog.int4)
RETURNS pg_catalog.int4 STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION pg_setinterval(pg_catalog.text, pg_catalog.int4)
RETURNS pg_catalog.int4 STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;

