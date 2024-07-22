package spark.analytics

import spark.SparkJob
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

object Task4 extends SparkJob {

  def main(args: Array[String]): Unit = {
    import spark.implicits._


    /**
     * select only stockcodes from orders which belongs to last month
     */
    val orders_df = spark.read.parquet(orders_gold_path)
      .select("stockcode","invoicedate")
      .withColumn("InvoiceDate", to_date(to_timestamp($"InvoiceDate", "MM/dd/yyyy HH:mm")))
      .withColumn("max_date", max($"InvoiceDate").over())
      .filter("year(InvoiceDate) == year(max_date) and month(InvoiceDate) == month(max_date)")
      .drop("InvoiceDate")
      .dropDuplicates()

    //get current as well as historical records from products dimension
    val products_df = spark.read.parquet(products_gold_path)
      .select("stockcode", "unitprice", "description", "eff_start_date", "eff_end_date")

    /**
     * Merge two data sets on basis of stock codes.
     * Filter to get records belong to last month along with current valid ones
     */
    val merge_df = products_df.join(orders_df, Seq("stockcode"), "inner")
      .filter("(max_date between eff_start_date and eff_end_date) or eff_end_date = '2099-12-31' ")
      .drop("max_date")

    //divide merged data frame to get historical and current records
    val current_valid_products = merge_df.filter("eff_end_date = '2099-12-31'")
      .selectExpr("stockcode", "unitprice as current_unit_price", "description")

    val last_month_valid_products = merge_df.filter("eff_end_date != '2099-12-31'")
      .withColumn("max_eff_end_date", max($"eff_end_date").over())
      .filter("max_eff_end_date == eff_end_date")
      .selectExpr("stockcode", "unitprice as old_unit_price", "description")

    //join historical and current records and get difference of unit price
    val task4_result_df = current_valid_products.join(last_month_valid_products, Seq("stockcode", "description"), "left")
      .selectExpr("stockcode",
        "description",
        "(coalesce(current_unit_price,0) - coalesce(old_unit_price,0)) as drop_diff"
      ).selectExpr(
        "dense_rank() over(order by drop_diff desc) as rank",
        "stockcode",
        "description",
        "drop_diff"
      )
      .filter("rank < 4")

    task4_result_df.show(false)
    task4_result_df.write.mode("overwrite").option("header", "true").csv(task4_result_path)


  }

}
