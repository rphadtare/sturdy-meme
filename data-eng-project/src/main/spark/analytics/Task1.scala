package spark.analytics

import spark.SparkJob

object Task1 extends SparkJob {

  /**
   *
   * Task1 - Top 10 countries with the most number of customers.
   */

  def main(args: Array[String]): Unit = {

    spark.read.parquet(customer_gold_path).createOrReplaceTempView("v_customer_dim")

    val task1_result_df = spark.sql(
    """
       select country,
       count_of_customers,
       dense_rank() over(order by count_of_customers desc) as rank_of_country from
        ( select country, count(1) as count_of_customers from v_customer_dim group by country
        ) as t
    """
    ).filter("rank_of_country < 11").select("country", "count_of_customers")

    task1_result_df.show(false)
    task1_result_df.write.mode("overwrite").option("header", "true").csv(task1_result_path)

  }

}
