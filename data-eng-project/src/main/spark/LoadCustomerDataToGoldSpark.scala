package spark

object LoadCustomerDataToGoldSpark extends SparkJob {

  def main(args: Array[String]): Unit = {
    spark.read.csv(customer_bronze_path).printSchema()

  }

}
