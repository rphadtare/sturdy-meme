package spark

import org.apache.spark.sql.SparkSession

class SparkJob {

  val spark: SparkSession = getSparkSession()

  private val bronze_layer = "data-eng-project/src/main/resources/bronze/"
  private val silver_layer = "data-eng-project/src/main/resources/silver/"
  private val gold_layer = "data-eng-project/src/main/resources/gold/"

  val customer_bronze_path = s"$bronze_layer/customer/"
  val products_bronze_path = s"$bronze_layer/products/"
  val orders_bronze_path = s"$bronze_layer/orders/"

  val customer_silver_path = s"$silver_layer/customer/"
  val products_silver_path = s"$silver_layer/products/"
  val orders_silver_path = s"$silver_layer/orders/"

  val customer_gold_path = s"$gold_layer/customer/"
  val products_gold_path = s"$gold_layer/products/"
  val orders_gold_path = s"$gold_layer/orders/"

  private def getSparkSession() : SparkSession = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    spark
  }

}
