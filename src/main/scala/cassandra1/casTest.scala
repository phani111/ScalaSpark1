package cassandra1

/**
  * Created by hadoop on 9/5/17.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql._

object casTest {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("cassandraspark").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder().master("local[*]").appName("CQL").config("spark.cassandra.connection.host", "localhost").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("cassandraspark").setMaster("local[*]")
        //val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data = args(0)
    val asldf = spark.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load(data)
    asldf.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).option("keyspace", "mykeyspace").option("table","mytable").save()
    val df =spark.read.format("org.apache.spark.sql.cassandra").option("keyspace", "mykeyspace").option("table","mytable").load()
    df.show()

    spark.stop()
  }
}
