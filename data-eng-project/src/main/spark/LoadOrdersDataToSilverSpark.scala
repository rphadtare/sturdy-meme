package spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object LoadOrdersDataToSilverSpark extends SparkJob {

  def main(args: Array[String]): Unit = {

    import spark.implicits._

    //referring current order data
    val orderDf = spark.read.option("header", "true").csv(orders_bronze_path)
    println(s"Input Count ${orderDf.count()}")
    val all_cols = orderDf.columns

    val customer_dim_df = spark.read.parquet(customer_gold_path)
    val products_dim_df = spark.read.parquet(products_gold_path)
      .where("eff_end_date > current_date")

    println(s"products_dim_df Count ${products_dim_df.count()}")

    /**
     * validating order records
     *
     * filtering records whose quantity is less than 0 or customer id is null
     * dropping overall duplicates
     */

    val validated_orders_df = orderDf
      .filter("Quantity > 0 and CustomerID is not null")
      .dropDuplicates()

    println(s"validated_orders_df Count - ${validated_orders_df.count()}")

    //filtering orders whose customer id is not valid
    val orders_by_valid_customers_df = validated_orders_df
      .join(customer_dim_df.select("customerid"), Seq("customerid"), "inner")

    println(s"orders_by_valid_customers_df count - ${orders_by_valid_customers_df.count()}")

    //filtering orders whose stockcode is not valid
    val orders_by_valid_product_df = orders_by_valid_customers_df
      .join(products_dim_df.select("StockCode"), Seq("StockCode"), "inner")

    println(s"orders_by_valid_product_df count - ${orders_by_valid_product_df.count()}")


    /**
     * aggregating data on basis of invoiceno, stockcode, InvoiceDate, customerid
     * i.e. aggregate quantity on basis of other coulmns
     */
    val columns_except_quantity = all_cols.filter(r => !r.equalsIgnoreCase("Quantity"))
    val aggregated_orders_df = orders_by_valid_product_df
      .withColumn("quantity", $"quantity".cast(IntegerType))
      .groupBy(columns_except_quantity.head, columns_except_quantity.tail:_*)
      .agg(sum("quantity").cast(IntegerType).alias("Quantity"))
      .select(all_cols.head, all_cols.tail:_*)

    println(s"aggregated_orders_df count - ${aggregated_orders_df.count()}")

    //aggregated_orders_df.withColumn("invoice_length", length($"InvoiceNo")).show()

    //storing data into silver layer
    aggregated_orders_df.write
      .option("header", "true")
      .mode("overwrite")
      .csv(orders_silver_path)

  }
}
