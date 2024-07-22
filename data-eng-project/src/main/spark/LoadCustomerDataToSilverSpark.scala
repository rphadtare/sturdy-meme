package spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object LoadCustomerDataToSilverSpark extends SparkJob {

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val customerDf = spark.read.option("header", "true").csv(customer_bronze_path)

    //to get information about all countries
    val all_country_df = ujson.read(requests.get("https://restcountries.com/v3.1/all")).arr
      .map(r => (r("name")("common").toString, r("cca3").toString, r("continents")(0)))
      .toArray.toList.map(r =>
        country(r._1.replace("\"", ""), r._2.replace("\"", ""), r._3.toString.replace("\"", "")))
      .toDF("country", "country_code", "region")

    //to clean source data and validate accordingly
    val windowSpec = Window.partitionBy("customerid").orderBy($"country".desc)

    val filter_customer_df = customerDf.filter("customerid is not null and country is not null")
      .filter("customerid rlike '[0-9]{0,5}'")
      .withColumn("rank", dense_rank().over(windowSpec))
      .filter("rank == 1").drop("rank")
      .withColumn("customerid", trim($"customerid"))
      .withColumn("country", trim($"country"))
      .withColumn("country", when($"country" === "EIRE", "Ireland")
        .when($"country" === "USA", "United States")
        .when($"country" === "RSA", "South Africa")
        .otherwise($"country"))

    //records - 'Unspecified', European Community' and 'Channel Islands' are excluded as they are not valid countries
    val customer_silver_df = filter_customer_df.join(all_country_df, Seq("country"), "inner")
      .select("customerid", "country", "country_code", "region")

    //customer_silver_df.show()

    //storing result to silver layer for customer data
    customer_silver_df.coalesce(1).write
      .option("header", "true")
      .mode("overwrite")
      .csv(customer_silver_path)

  }

}
