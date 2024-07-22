package spark.analytics

import spark.SparkJob
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Task3 extends SparkJob {

  /**
   * Task3 - Relationship between average unit price of products and their sales volume.
   */

  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val products_df = spark.read.parquet(products_gold_path)
      .where("eff_end_date > current_date")
      .selectExpr("stockcode", "unitprice", "round(avg(unitprice) over(), 2) as avg_unit_price")

    val orders_df = spark.read.parquet(orders_gold_path)
      .select("stockcode", "quantity")

    val task3_result_df = products_df.join(orders_df, Seq("stockcode"), "left")
      .groupBy("stockcode", "unitprice", "avg_unit_price")
      .agg(sum("quantity").alias("sales"))
      .withColumn("total_sales", round(sum($"sales").over(),2))
      .selectExpr(
        "case when unitprice > avg_unit_price then 'Above avg value product' " +
          "when unitprice == avg_unit_price then 'Avg value product' " +
          "else 'Below avg value product' end as product_group",
        "sales",
        "total_sales",
        "avg_unit_price")
      .groupBy("product_group", "avg_unit_price", "total_sales")
      .agg(sum("sales").alias("sales_by_product_group"))
      .sort($"sales_by_product_group".desc)

    task3_result_df.show(false)
    task3_result_df.write.mode("overwrite").option("header", "true").csv(task3_result_path)

  }

}
