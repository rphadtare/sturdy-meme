package spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.AnalysisException

object LoadProductsDataToGoldSpark extends SparkJob {

  def main(args: Array[String]): Unit = {
    import spark.implicits._

    //referring current data from silver layer
    val new_product_df = spark.read.option("header","true").csv(products_silver_path)
      .withColumn("eff_start_date", current_date)
      .withColumn("eff_end_date", to_date(lit("2099-12-31")))

    /**
     * Below code is to retrieve existing data present from gold layer.
     * If in case there is no data present, then will refer to dataframe structure from silver layer for first run
     */
    var existing_products_df = spark.emptyDataFrame
    try{
      existing_products_df = spark.read.format("parquet").load(products_gold_path)

      /**
       * If schema gets evolved, then this section will allow schema evolution without any changes
       */
      if(new_product_df.columns.size > existing_products_df.columns.size){
        for(column_name <- new_product_df.columns if !existing_products_df.columns.contains(column_name)){
          existing_products_df = existing_products_df.withColumn(column_name, lit(null))
        }
      }

    } catch{
      case ex:AnalysisException =>
        print(ex.getMessage)
        existing_products_df = new_product_df.limit(0)
    }

    /**
     * SCD type 2 implementation -
     * Logic to identify, if is there any change to existing record present in gold layer as compared to silver layer
     * If new change occurs to existing one. Then existing record will get discontinued by marking 'eff_end_date' as 'current date'
     *
     * If no new change occurs to existing one as well as no new record is present in new batch.
     * Then, script will not execute re-insertion logic.
     * */
    val all_cols = new_product_df.columns
    val columns_except_start_date = all_cols.filter(r => !r.equalsIgnoreCase("eff_start_date"))

    val valid_new_products_df = new_product_df
      .select(columns_except_start_date.head, columns_except_start_date.tail:_*)
      .except(existing_products_df.select(columns_except_start_date.head, columns_except_start_date.tail:_*))
      .withColumn("eff_start_date", current_date)
      .select(all_cols.head,  all_cols.tail:_*)

    //if new valid changes are available then only execute below block
    //i.e. to store existing and new changes together

    if(!valid_new_products_df.isEmpty){
      val final_df = valid_new_products_df.union(existing_products_df)
        .withColumn("rank", dense_rank.over(Window.partitionBy("stockcode").orderBy($"eff_start_date".desc)))
        .withColumn("eff_end_date", when($"rank" > 1, current_date).otherwise($"eff_end_date"))
        .drop("rank")

      //display(final_df)

      final_df.coalesce(1).write.format("parquet").mode("overwrite")
        .option("overwriteSchema", "true")
        .save(products_gold_path)
    }


  }

}
