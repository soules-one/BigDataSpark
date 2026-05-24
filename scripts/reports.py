from pyspark.sql import SparkSession

def save_report(df, db_table_name, spark):
    """
    Универсальная функция для записи DataFrame в ClickHouse и MongoDB.
    """
    df = df.coalesce(1)
    
    try:
        clickhouse_path = f"clickhouse.default.{db_table_name}"
        spark.sql(f"DROP TABLE IF EXISTS {clickhouse_path}")
        df.writeTo(clickhouse_path).create()
        print(f"[OK] ClickHouse: {clickhouse_path}")
    except Exception as e:
        print(f"[ERROR] ClickHouse ({clickhouse_path}): {e}")

    try:
        (
            df.write
            .format("mongodb")
            .mode("overwrite")
            .option("database", "petstore_reports")
            .option("collection", db_table_name)
            .save()
        )
        print(f"[OK] MongoDB: petstore_reports.{db_table_name}")
    except Exception as e:
        print(f"[ERROR] MongoDB ({db_table_name}): {e}")


def main():
    spark = (
        SparkSession.builder
        .appName("ETL_Star_to_Reports_Docker")
        .config("spark.jars", ",".join([
            "/opt/spark/extra-jars/postgresql-42.7.3.jar",
            "/opt/spark/extra-jars/clickhouse-spark-runtime-3.5_2.12-0.10.0.jar",
            "/opt/spark/extra-jars/mongo-spark-connector_2.12-10.4.0.jar",
            "/opt/spark/extra-jars/mongodb-driver-sync-5.1.0.jar",
            "/opt/spark/extra-jars/mongodb-driver-core-5.1.0.jar",
            "/opt/spark/extra-jars/bson-5.1.0.jar",
        ]))
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.mongodb.write.connection.uri", "mongodb://mongodb_reports:27017")
        .config("spark.mongodb.write.database", "petstore_reports")
        .config("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
        .config("spark.sql.catalog.clickhouse.host", "clickhouse-db")
        .config("spark.sql.catalog.clickhouse.protocol", "http")
        .config("spark.sql.catalog.clickhouse.http_port", "8123")
        .config("spark.sql.catalog.clickhouse.user", "app")
        .config("spark.sql.catalog.clickhouse.password", "app")
        .config("spark.sql.catalog.clickhouse.database", "default")
        .getOrCreate()
    )

    PG_URL = "jdbc:postgresql://postgres_dw:5432/app"
    PG_PROPS = {
        "user": "app",
        "password": "app",
        "driver": "org.postgresql.Driver"
    }

    tables = ["dim_date", "dim_customer", "dim_product", "dim_store", "dim_supplier", "fact_sales"]
    for tbl in tables:
        df = spark.read.jdbc(url=PG_URL, table=tbl, properties=PG_PROPS)
        df.createOrReplaceTempView(tbl)

    sql_datamarts = {
        "mart_product_sales": """
            WITH product_sales AS (
                SELECT p.product_id, p.product_name, p.product_category,
                       SUM(f.total_price) AS total_revenue,
                       SUM(f.quantity) AS items_sold,
                       AVG(p.product_rating) AS avg_rating,
                       MAX(p.product_reviews) AS total_reviews
                FROM fact_sales f 
                JOIN dim_product p ON f.product_id = p.product_id
                GROUP BY p.product_id, p.product_name, p.product_category
            ),
            category_sales AS (
                SELECT product_category, SUM(total_price) AS category_revenue
                FROM fact_sales f JOIN dim_product p ON f.product_id = p.product_id
                GROUP BY product_category
            )
            SELECT ps.product_id, ps.product_name, ps.product_category,
                   ps.total_revenue, ps.items_sold, 
                   cs.category_revenue, ps.avg_rating, ps.total_reviews
            FROM product_sales ps
            JOIN category_sales cs ON ps.product_category = cs.product_category
            ORDER BY ps.total_revenue DESC
        """,
        "mart_customer_sales": """
            SELECT c.customer_id, c.first_name, c.last_name, c.country,
                   SUM(f.total_price) AS total_spent,
                   AVG(f.total_price) AS avg_check,
                   COUNT(f.sale_id) AS total_orders
            FROM fact_sales f 
            JOIN dim_customer c ON f.customer_id = c.customer_id
            GROUP BY c.customer_id, c.first_name, c.last_name, c.country
            ORDER BY total_spent DESC
        """,
        "mart_time_sales": """
            WITH monthly AS (
                SELECT d.year, d.month,
                       SUM(f.total_price) AS monthly_revenue,
                       AVG(f.total_price) AS avg_order_size
                FROM fact_sales f 
                JOIN dim_date d ON f.date_id = d.date_id
                GROUP BY d.year, d.month
            )
            SELECT year, month, 
                   monthly_revenue, 
                   avg_order_size,
                   LAG(monthly_revenue) OVER (ORDER BY year, month) AS prev_month_revenue,
                   (monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY year, month)) AS revenue_diff
            FROM monthly
            ORDER BY year, month
        """,
        "mart_store_sales": """
            SELECT s.store_id, s.country, s.city,
                   SUM(f.total_price) AS store_revenue,
                   AVG(f.total_price) AS avg_check,
                   COUNT(f.sale_id) AS total_orders
            FROM fact_sales f 
            JOIN dim_store s ON f.store_id = s.store_id
            GROUP BY s.store_id, s.country, s.city
            ORDER BY store_revenue DESC
        """,
        "mart_supplier_sales": """
            SELECT s.supplier_id, s.country AS supplier_country,
                   SUM(f.total_price) AS supplier_revenue,
                   AVG(f.total_price / f.quantity) AS avg_item_price
            FROM fact_sales f 
            JOIN dim_supplier s ON f.supplier_id = s.supplier_id
            GROUP BY s.supplier_id, s.country
            ORDER BY supplier_revenue DESC
        """,
        "mart_product_quality": """
            SELECT p.product_id, p.product_name,
                   p.product_rating, p.product_reviews,
                   SUM(f.quantity) AS sales_volume,
                   SUM(f.total_price) AS revenue
            FROM dim_product p
            LEFT JOIN fact_sales f ON p.product_id = f.product_id
            GROUP BY p.product_id, p.product_name, p.product_rating, p.product_reviews
            ORDER BY p.product_rating DESC, p.product_reviews DESC
        """
    }

    sql_sub_views = {
        # Витрина 1: Продукты
        "view_product_top10_sales": "SELECT product_id, product_name, total_revenue, items_sold FROM mart_product_sales ORDER BY total_revenue DESC LIMIT 10",
        "view_product_category_revenue": "SELECT product_category, MAX(category_revenue) as category_revenue FROM mart_product_sales GROUP BY product_category ORDER BY category_revenue DESC",
        "view_product_avg_rating": "SELECT product_id, product_name, avg_rating, total_reviews FROM mart_product_sales",
        
        # Витрина 2: Клиенты
        "view_customer_top10_spenders": "SELECT customer_id, first_name, last_name, total_spent FROM mart_customer_sales ORDER BY total_spent DESC LIMIT 10",
        "view_customer_country_dist": "SELECT country, COUNT(customer_id) AS customer_count FROM mart_customer_sales GROUP BY country ORDER BY customer_count DESC",
        "view_customer_avg_check": "SELECT customer_id, first_name, last_name, avg_check FROM mart_customer_sales",
        
        # Витрина 3: Время
        "view_time_monthly_trends": "SELECT year, month, monthly_revenue FROM mart_time_sales ORDER BY year, month",
        "view_time_yearly_comparison": "SELECT year, SUM(monthly_revenue) AS yearly_revenue FROM mart_time_sales GROUP BY year ORDER BY year",
        "view_time_avg_order": "SELECT year, month, avg_order_size FROM mart_time_sales ORDER BY year, month",
        
        # Витрина 4: Магазины
        "view_store_top5_revenue": "SELECT store_id, store_revenue FROM mart_store_sales ORDER BY store_revenue DESC LIMIT 5",
        "view_store_sales_by_geo": "SELECT country, city, SUM(store_revenue) AS geo_revenue FROM mart_store_sales GROUP BY country, city ORDER BY geo_revenue DESC",
        "view_store_avg_check": "SELECT store_id, avg_check FROM mart_store_sales",
        
        # Витрина 5: Поставщики
        "view_supplier_top5_revenue": "SELECT supplier_id, supplier_revenue FROM mart_supplier_sales ORDER BY supplier_revenue DESC LIMIT 5",
        "view_supplier_avg_price": "SELECT supplier_id, avg_item_price FROM mart_supplier_sales",
        "view_supplier_sales_by_country": "SELECT supplier_country, SUM(supplier_revenue) AS total_revenue FROM mart_supplier_sales GROUP BY supplier_country ORDER BY total_revenue DESC",
        
        # Витрина 6: Качество
        "view_quality_extremes": """
            SELECT 'Highest' AS type, product_name, product_rating FROM (SELECT product_name, product_rating FROM mart_product_quality ORDER BY product_rating DESC LIMIT 5)
            UNION ALL
            SELECT 'Lowest' AS type, product_name, product_rating FROM (SELECT product_name, product_rating FROM mart_product_quality ORDER BY product_rating ASC LIMIT 5)
        """,
        "view_quality_most_reviews": "SELECT product_name, product_reviews FROM mart_product_quality ORDER BY product_reviews DESC LIMIT 10"
    }

    print("\n--- Запуск формирования витрин (Marts) ---")
    for report_name, query in sql_datamarts.items():
        print(f"\nФормирование: {report_name}")
        df = spark.sql(query)
        save_report(df, report_name, spark)
        df.createOrReplaceTempView(report_name)

    print("\n--- Запуск формирования отчетов (Views) ---")
    for view_name, query in sql_sub_views.items():
        print(f"\nФормирование подзадачи: {view_name}")
        df_view = spark.sql(query)
        save_report(df_view, view_name, spark)

    print("\nФормирование вспомогательной таблицы (Корреляция для качества)...")
    df_quality = spark.table("mart_product_quality")
    
    try:
        corr_value = df_quality.stat.corr("product_rating", "sales_volume")
        df_corr_res = spark.createDataFrame([(float(corr_value),)], ["rating_sales_correlation"])
    except Exception as e:
        print(f"Warning: cannot compute correlation: {e}")
        df_corr_res = spark.createDataFrame([], "rating_sales_correlation double")
        
    save_report(df_corr_res, "aux_quality_correlation", spark)

    print("\n--- Формирование витрин и отчетов завершено ---")
    spark.stop()

if __name__ == "__main__":
    main()