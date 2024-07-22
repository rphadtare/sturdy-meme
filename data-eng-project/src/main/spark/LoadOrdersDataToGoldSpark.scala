package spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.AnalysisException

object LoadOrdersDataToGoldSpark extends SparkJob {

  def main(args: Array[String]): Unit = {

    import spark.implicits._
    spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

    //referring current order data from silver layer
    val orders_silver_df = spark.read.option("header", "true").csv(orders_silver_path)

    var existing_orders_df = spark.emptyDataFrame

    try{
      existing_orders_df = spark.read.parquet(orders_gold_path)
    } catch{
      case ex:AnalysisException =>
        print(ex.getMessage)
        existing_orders_df = orders_silver_df.limit(0)
    }

    val merged_orders_df = orders_silver_df.union(existing_orders_df).dropDuplicates()
      .groupBy("InvoiceNo", "StockCode", "CustomerID")
      .agg(sum("Quantity").cast(IntegerType).alias("Quantity"), max("InvoiceDate").alias("InvoiceDate"))
      .withColumn("InvoiceDate", to_timestamp($"InvoiceDate", "MM/dd/yyyy HH:mm"))

    //display(merged_orderes_df)
    println(s"merged_orders_df count - ${merged_orders_df.count}")

    merged_orders_df.write
      .mode("overwrite")
      .parquet(orders_gold_path)


  }

}
