package spark.silver_to_gold

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.functions._
import spark.SparkJob

object LoadCustomerDataToGoldSpark extends SparkJob {

  def main(args: Array[String]): Unit = {

    import spark.implicits._

    val customer_dim_silver_df = spark.read.option("header", "true")
      .csv(customer_silver_path)
      .withColumn("record_insert_date", current_date)
      .withColumn("record_update_date", current_date)

    var customer_dim_gold_df = spark.emptyDataFrame

    //check if data is present in gold layer or not
    //if not then consider it as first run
    try{
      customer_dim_gold_df = spark.read.format("parquet").load(customer_gold_path)

    } catch {
      case ex: AnalysisException =>
        println(ex.getMessage)
        customer_dim_gold_df = customer_dim_silver_df.limit(0)
    }

    /**
     *  compare existing and new data to upsert
     *  While inserting customer records into final layer used two more attributes
     *  record_insert_date - capture date when new record is inserting
     *  record_update_date - capture date when existing record is modified
     *
     *  here, no history of customer data is maintained.
     *  SCD type 1 is implemented
     */

    val new_customer_ids = customer_dim_silver_df.select("customerid")
      .distinct.map(r => r.getString(0))
      .collectAsList

    println(new_customer_ids.size)

    val unchanged_customer_dim_records_df = customer_dim_gold_df
      .filter(r => !new_customer_ids.contains(r.getString(0)))

    //unchanged_customer_dim_records_df.count

    customer_dim_gold_df.filter(r => new_customer_ids.contains(r.getString(0)))
      .createOrReplaceTempView("existing_customers")

    customer_dim_silver_df.createOrReplaceTempView("new_customers")

    val changed_customer_dim_records_df = spark.sql(
    """
       select t1.customerid,
              t1.country,
              t1.country_code,
              t1.region,
              coalesce(t2.record_insert_date, t1.record_insert_date) as record_insert_date,
              t1.record_update_date
       from new_customers t1 left join existing_customers t2
       on t1.customerid = t2.customerid
    """)

    val final_customer_df = changed_customer_dim_records_df.union(unchanged_customer_dim_records_df)
    //final_customer_df.show(false)

    final_customer_df.coalesce(1).write.format("parquet")
      .mode("overwrite")
      .option("compression", "snappy")
      .save(customer_gold_path)

    spark.close()

  }

}
