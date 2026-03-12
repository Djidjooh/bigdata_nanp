from pyspark.sql import SparkSession
# Configuration des versions
ICEBERG_VERSION = "3.4_2.12" # Pour Spark 3.5
NESSIE_VERSION = "0.99.0"

#MINIO
MINIO_SERVER = "http://minio:9000"
MINIO_USER = "admin"
MINIO_PASS = "password123"

# NESSIE
NESSIE_HOST = "nessie"
NESSIE_URI = "http://catalog:19120/api/v1"
NESSIE_WH = "s3://nessie-bucket/warehouse/"

# DATA STORE
NESSIE_DATA_STORE = "s3://silver-test-bucket/warehouse/db_prod/"


spark = SparkSession.builder \
    .appName("Nessie-Iceberg-Client") \
    .config("spark.jars.packages", 
            "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,org.apache.iceberg:iceberg-aws-bundle:1.10.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.4_2.12-0.99.0,") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.nessie.uri", NESSIE_URI) \
    .config("spark.sql.catalog.nessie.ref", "main") \
    .config("spark.sql.catalog.nessie.warehouse", NESSIE_WH) \
    .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
    .config("spark.sql.catalog.nessie.s3.endpoint", MINIO_SERVER) \
    .config("spark.sql.catalog.nessie.s3.access-key-id", MINIO_USER) \
    .config("spark.sql.catalog.nessie.s3.secret-access-key", MINIO_PASS) \
    .config("spark.sql.catalog.nessie.s3.path-style-access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.iceberg.read.split.target-size", "131072") \
    .config("spark.executor.memory", "800m") \
    .config("spark.executor.cores", "1") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.iceberg.vectorization.enabled", "false") \
    .config("spark.sql.iceberg.distribution-mode", "hash") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()
    
#-- Création du schéma dans Nessie
spark.sql(f"""
CREATE NAMESPACE IF NOT EXISTS nessie.db_prod 
LOCATION '{NESSIE_DATA_STORE}'
""")

# Vérification
spark.sql("DESCRIBE NAMESPACE nessie.db_prod").show()

# Exemple : Création d'une table et insertion
spark.sql("CREATE TABLE IF NOT EXISTS nessie.db_prod.pyspark_test (id INT, data STRING) USING iceberg")
spark.sql("INSERT INTO nessie.db_prod.pyspark_test VALUES (1, 'Hello from PySpark')")

# Lecture
spark.sql("SELECT * FROM nessie.db_prod.pyspark_test").show()
