package bank

/**
  * Created by hadoop on 23/4/17.
  */

import org.apache.spark.sql.SparkSession

object CSVTest {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("Jsontest").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("Jsontest").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    //import spark.sql
    val data = args(0)
    val data1 = args(1)
    //val df = spark.read.json(data)
    val df1 = spark.read.format("com.databricks.spark.csv").option("header", "true").load(data1)
    //df1.printSchema()
    df1.createOrReplaceTempView("tab")
    val res = spark.sql("select distinct county from tab ")
    res.show()


    spark.stop()
  }
}