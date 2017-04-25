package bank

/**
  * Created by hadoop on 24/4/17.
  */

import java.util.Properties

import org.apache.spark.sql.SparkSession

object SparkOracle {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("SparkOracle").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("SparkOracle").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    import java.util.Properties
    val ohost = "jdbc:oracle:thin://@orcldb.cngjcoshs6ee.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val oprop = new Properties
    oprop.setProperty("driver","oracle.jdbc.OracleDriver")
    oprop.setProperty("user","ousername")
    oprop.setProperty("password","opassword")

    val odf = spark.read.jdbc(ohost,"DEPT",oprop)
    val odf1 = spark.read.jdbc(ohost,"EMP",oprop)
    //odf.show()
    //val result = odf.where("DEPTNO > 20")
    //result.write.csv("file:///home/hadoop/result/oracle1")
    //result.write.json("file:///home/hadoop/result/json")
    odf.createOrReplaceTempView("DEPT")
    odf1.createOrReplaceTempView("EMP")
    val result2 = spark.sql("SELECT e.DEPTNO, d.DNAME, sum(e.SAL) Salary FROM EMP e  inner join DEPT d on e.DEPTNO = d.DEPTNO\ngroup by e.DEPTNO, d.DNAME ")
    result2.show()
    result2.coalesce(1).write.parquet("file:///home/hadoop/result/join3")
    //result2.write.jdbc(ohost,"TEST",oprop)
    spark.stop()
  }
}