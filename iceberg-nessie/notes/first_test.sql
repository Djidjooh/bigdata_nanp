-- read 'notes.txt' to understand why we use 'nessie.'
-- WITH (location = 's3://silver-test-bucket/warehouse/db_prod/'); is where your data will reside on minio
-- create bucket 'silver-test-bucket' before
-- Se connecte directement au catalogue iceberg nessie
-- docker exec -it trino trino --catalog nessie
-- 1. Création du schéma dans Nessie
CREATE SCHEMA nessie.db_prod 
WITH (location = 's3://silver-test-bucket/warehouse/db_prod/');

-- 2. Création d'une table optimisée pour vos petits fichiers
CREATE TABLE nessie.db_prod.sensor_data (
    sensor_id VARCHAR,
    value DOUBLE,
    ts TIMESTAMP(3)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(ts)'],
    -- Force la taille des fichiers à être plus petite pour vos tests
    parquet_writer_block_size = '32MB'
);

-- 3. Insertion de test
INSERT INTO nessie.db_prod.sensor_data 
VALUES ('sensor_01', 25.5, TIMESTAMP '2026-03-12 18:00:00');

-- 4. Vérification
SELECT * FROM nessie.db_prod.sensor_data;

-- 5. croner pour optimisation
-- Fusionne les petits fichiers en fichiers plus gros (à faire périodiquement)
ALTER TABLE votre_table EXECUTE optimize;
-- Nettoie les anciens snapshots pour alléger MariaDB/Nessie
ALTER TABLE votre_table EXECUTE expire_snapshots(retention_threshold => '2d');
