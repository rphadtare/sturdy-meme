package spark.analytics

import spark.SparkJob
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Task2 extends SparkJob {

  /**
   *
   * Task2 - Revenue distribution by country.
   */

  def main(args: Array[String]): Unit = {

    import spark.implicits._

    val customer_df = spark.read.parquet(customer_gold_path)

    val orders_df = spark.read.parquet(orders_gold_path)

    val products_df = spark.read.parquet(products_gold_path)
      .where("eff_end_date > current_date")
      .select("StockCode", "unitprice")

    val merge_order_product_df = orders_df.join(products_df, Seq("StockCode"), "inner")
      .withColumn("total_sales", $"Quantity" * $"unitprice")
      .select("total_sales", "CustomerID")
      .groupBy("CustomerID").agg(round(sum("total_sales"), 2).alias("total_sales"))

    //display(merge_order_product_df)
    val task2_result_df = merge_order_product_df.join(customer_df, Seq("customerid"), "inner")
      .select("country", "total_sales")
      .groupBy("country").agg(round(sum("total_sales"), 2).alias("revenue_contribution"))
      .withColumn("total_revenue", round(sum($"revenue_contribution").over(), 2))
      .withColumn("contribution_percentage", expr("round((revenue_contribution/total_revenue)*100, 3)"))
      .sort($"contribution_percentage".desc)

    task2_result_df.show(false)
    task2_result_df.write.mode("overwrite").option("header", "true").csv(task2_result_path)

  }

}
