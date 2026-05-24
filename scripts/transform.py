from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import glob

spark = (
    SparkSession.builder
    .appName("ETL_Raw_to_Star_Docker")
    .getOrCreate()
)

PG_URL = "jdbc:postgresql://postgres_dw:5432/app"
PG_PROPS = {
    "user": "app",
    "password": "app",
    "driver": "org.postgresql.Driver"
}

files = glob.glob("/opt/spark/work-dir/data/MOCK_DATA*.csv")

df_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(files)
)

df_raw = (
    df_raw
    .withColumn("id", F.col("id").cast("int"))
    .withColumn("sale_customer_id", F.col("sale_customer_id").cast("int"))
    .withColumn("sale_product_id", F.col("sale_product_id").cast("int"))
    .withColumn("sale_quantity", F.col("sale_quantity").cast("int"))
    .withColumn("sale_total_price", F.col("sale_total_price").cast("double"))
    .withColumn("customer_age", F.col("customer_age").cast("int"))
    .withColumn("product_price", F.col("product_price").cast("double"))
    .withColumn("product_rating", F.col("product_rating").cast("double"))
    .withColumn("product_reviews", F.col("product_reviews").cast("int"))
    .withColumn("sale_date", F.to_date(F.col("sale_date"), "M/d/yyyy"))
)

df_raw.createOrReplaceTempView("raw_data")

dim_date = spark.sql("""
    SELECT DISTINCT
        CAST(date_format(sale_date, 'yyyyMMdd') AS INT) AS date_id,
        sale_date AS full_date,
        year(sale_date) AS year,
        month(sale_date) AS month
    FROM raw_data
    WHERE sale_date IS NOT NULL
""")

dim_customer = spark.sql("""
    SELECT DISTINCT
        sale_customer_id AS customer_id,
        customer_first_name AS first_name,
        customer_last_name AS last_name,
        customer_age AS age,
        customer_country AS country,
        customer_pet_type AS pet_type
    FROM raw_data
    WHERE sale_customer_id IS NOT NULL
""")

dim_product = spark.sql("""
    SELECT DISTINCT
        sale_product_id AS product_id,
        product_name,
        product_category,
        product_brand,
        product_price,
        product_rating,
        product_reviews
    FROM raw_data
    WHERE sale_product_id IS NOT NULL
""")

dim_store = spark.sql("""
    SELECT DISTINCT
        store_name AS store_id,
        store_city AS city,
        store_country AS country
    FROM raw_data
    WHERE store_name IS NOT NULL
""")

dim_supplier = spark.sql("""
    SELECT DISTINCT
        supplier_name AS supplier_id,
        supplier_country AS country
    FROM raw_data
    WHERE supplier_name IS NOT NULL
""")

fact_sales = spark.sql("""
    SELECT
        id AS sale_id,
        CAST(date_format(sale_date, 'yyyyMMdd') AS INT) AS date_id,
        sale_customer_id AS customer_id,
        sale_product_id AS product_id,
        store_name AS store_id,
        supplier_name AS supplier_id,
        sale_quantity AS quantity,
        sale_total_price AS total_price
    FROM raw_data
    WHERE sale_date IS NOT NULL
""")

dims = {
    "dim_date": dim_date,
    "dim_customer": dim_customer,
    "dim_product": dim_product,
    "dim_store": dim_store,
    "dim_supplier": dim_supplier,
    "fact_sales": fact_sales
}

for table_name, df in dims.items():
    print(f"Пишем таблицу: {table_name}")
    df.write.jdbc(url=PG_URL, table=table_name, mode="overwrite", properties=PG_PROPS)

spark.stop()