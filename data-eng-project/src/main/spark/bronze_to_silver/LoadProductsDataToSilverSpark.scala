package spark.bronze_to_silver

import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.SparkJob

object LoadProductsDataToSilverSpark extends SparkJob {

  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val productsDf = spark.read.option("header", "true").csv(products_bronze_path)

    @transient val spec = Window.partitionBy($"description").orderBy($"stockcode".desc)

    /**
     *  select products with valid stock code, description and unit price.
     *  removing duplication by selecting biggest stockcode where description is exact same
     */

    val new_products_df = productsDf
      .filter("""
            stockcode is not null and unitprice is not null
            and stockcode rlike '[0-9]{5}[a-bA-Z]{0,1}'
            and ( description rlike '[A-Z0-9]+' and description not rlike '[a-z]+')
        """)
      .withColumn("description", regexp_replace(trim($"description"), "\"", ""))
      .withColumn("rank", dense_rank().over(spec))
      .filter("rank == 1")
      .drop("rank")

    /**
     * logic to remove duplicated stock codes e.g. 23236,
     * select record whose description value is smallest among them
     *
     * Also, for few records values are duplicated across stockcode and description.
     * For such cases, removing duplicate records by selecting minimum unit price record
     */
    @transient val dupl_spec = Window.partitionBy($"stockcode").orderBy($"description", $"unitprice")

    val validated_df = new_products_df
      .withColumn("rank", dense_rank().over(dupl_spec))
      .filter("rank == 1")
      .drop("rank")
      .withColumn("unitprice", $"unitprice".cast(FloatType))
      .filter("unitprice > 0")

    /**
     * Below section is to calculate product category using stockcode and description
     * Here, I'm trying to group few values based on common parts present in stock code and description
     * e.g. product code - if stockcode is '12345A' then product code is '12345'
     *
     *
     * i.e. if two products having common string in their description as well as in stockcode like below  -
     * 15034	PAPER POCKET TRAVELING FAN	1.27
     * 15036	ASSORTED COLOURS SILK FAN	  1.74
     *
     * then
     * product code - 1503
     * product_category - FAN
     */


    val productDf_with_pc = validated_df.withColumn(
      "product_code",
      when($"stockcode" rlike "[0-9A-Za-z]{6,}", substring($"stockcode", 0, 5))
        .otherwise(expr("substring(stockcode, 0, length(stockcode)-1)"))
    )

    val productDf_with_prod_category = productDf_with_pc
      .withColumn("description_arr", split($"description", " "))
      .sort($"product_code", $"description_arr")
      .select("product_code", "description_arr")
      .groupBy("product_code")
      .agg(
        expr("""
        aggregate(
            collect_list(description_arr),
            collect_list(description_arr)[0],
            (acc, x) -> array_intersect(acc, x)
        ) as product_category
    """)
      ).withColumn("product_category", concat_ws(" ", $"product_category"))


    /**
     * Here, populating product category same as description for those records
     * whose category information was not able found via above approach
     */

    val final_product_df = productDf_with_pc
      .join(productDf_with_prod_category, Seq("product_code"), "inner")
      .selectExpr("stockcode",
        "description",
        "unitprice",
        "product_code",
        "case when product_category is null or length(product_category) == 0 " +
          "then description else product_category end as product_category"
      )

    final_product_df
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(products_silver_path)

    //final_product_df.groupBy("description").count().where("count > 1")



  }

}
