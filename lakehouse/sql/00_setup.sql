-- Connects DuckDB to the lakehouse. Python runs this on every connection, and you can
-- also use it from the DuckDB CLI:   duckdb -init lakehouse/sql/00_setup.sql
--
--   MinIO (S3)            holds the Parquet data + Iceberg metadata files
--   Iceberg REST catalog  tracks which metadata file is "current" for each table
--   DuckDB                is the query/compute engine (nothing is stored in DuckDB itself)

INSTALL iceberg;
INSTALL httpfs;
LOAD iceberg;
LOAD httpfs;

CREATE OR REPLACE SECRET minio (
    TYPE s3,
    KEY_ID 'admin',
    SECRET 'supersecurepassword123',
    ENDPOINT 'localhost:9000',
    URL_STYLE 'path',
    USE_SSL false,
    REGION 'us-east-1'
);

ATTACH IF NOT EXISTS 'lake' AS lake (
    TYPE iceberg,
    ENDPOINT 'http://localhost:8181',
    AUTHORIZATION_TYPE 'none'
);

-- Debezium encodes NUMERIC columns as base64 big-endian unscaled integers unless the
-- connector sets decimal.handling.mode=string. connector.json now uses "string"
-- ("499.00"), but events produced before that change are still in Kafka/bronze as base64
-- ("AMLs" = 0x00C2EC = 49900 -> 499.00). This macro reads both forms.
-- (Base64 never contains '.', and string mode always prints the scale, so it's unambiguous.
--  Negative base64 values are not handled; prices and totals here are never negative.)
CREATE OR REPLACE MACRO dbz_decimal(v, scale) AS
    CASE
        WHEN v IS NULL THEN NULL
        WHEN regexp_full_match(v, '-?[0-9]+\.[0-9]+') THEN CAST(v AS DECIMAL(18, 2))
        ELSE CAST(CAST('0x' || hex(from_base64(v)) AS HUGEINT) / power(10, scale) AS DECIMAL(18, 2))
    END;
