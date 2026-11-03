package org.marketplace;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.lit;

public final class SparkRecommendations {
    private SparkRecommendations() {
    }

    public static void main(String[] args) {
        String hdfs = System.getenv("HDFS_URI");
        if (hdfs == null || hdfs.isBlank()) {
            hdfs = "hdfs://localhost:9000";
        }
        SparkSession spark = SparkSession.builder()
                .appName("marketplace-recommendations")
                .config("spark.hadoop.fs.defaultFS", hdfs)
                .config("spark.ui.enabled", "false")
                .getOrCreate();
        Dataset<Row> products = spark.read().json(hdfs + "/data/products");
        Dataset<Row> top = products.select(
                        col("product_id"),
                        col("name"),
                        col("category"),
                        col("stock.available").alias("available"))
                .orderBy(desc("available"))
                .limit(3)
                .withColumn("reason", lit("Наибольший остаток среди разрешённых товаров"));
        top.coalesce(1).write().mode("overwrite").json(hdfs + "/data/recommendations");
        top.show(false);
        spark.stop();
    }
}
