package wordcount

/**
  * Created by hadoop on 15/4/17.
  */

import org.apache.spark.sql.SparkSession

object wordcount {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("worcount").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("wordcount").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    //import spark.implicits._
    import spark.sql
    val a = sc.textFile(args(0))
    val wc = a.flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey(_+_)
    wc.saveAsTextFile(args(1))

    spark.stop()
  }
}