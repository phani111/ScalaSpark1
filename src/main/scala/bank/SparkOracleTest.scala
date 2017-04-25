package bank

/**
  * Created by hadoop on 25/4/17.
  */

import org.apache.spark.sql.SparkSession

object SparkOracleTest {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("SparkOracleTest").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("SparkOracleTest").getOrCreate()
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

    val ohost1 = "jdbc:mysql://mysqldb.cngjcoshs6ee.ap-south-1.rds.amazonaws.com:3306/mydb"
    val oprop1 = new Properties
    oprop1.setProperty("driver","com.mysql.jdbc.Driver")
    oprop1.setProperty("user","ousername")
    oprop1.setProperty("password","opassword")

    val AGENTS = spark.read.jdbc(ohost,"AGENTS",oprop)
    val CUSTOMER = spark.read.jdbc(ohost,"CUSTOMER",oprop)
    val ORDERS = spark.read.jdbc(ohost,"ORDERS",oprop)
    //odf.show()
    //val result = odf.where("DEPTNO > 20")
    //result.write.csv("file:///home/hadoop/result/oracle1")
    //result.write.json("file:///home/hadoop/result/json")
    AGENTS.createOrReplaceTempView("AGENT")
    CUSTOMER.createOrReplaceTempView("CUSTOMER")
    ORDERS.createOrReplaceTempView("ORDERS")

    //ORDERS.write.jdbc(ohost1,"ORDERS",oprop1)

    //AGENTS.write.jdbc(ohost1,"AGENTS",oprop1)

    //CUSTOMER.write.jdbc(ohost1,"CUSTOMER",oprop1)

    val resultsql = spark.sql("SELECT a.ord_num,b.cust_name,a.cust_code,c.agent_code,b.cust_city FROM AGENT c,CUSTOMER b,ORDERS a WHERE b.cust_city=c.working_area AND a.cust_code=b.cust_code  \nAND a.agent_code=c.agent_code")

    //val result2 = spark.sql("SELECT e.DEPTNO, d.DNAME, sum(e.SAL) Salary FROM EMP e  inner join DEPT d on e.DEPTNO = d.DEPTNO\ngroup by e.DEPTNO, d.DNAME ")
    //result2.show()
    //result2.coalesce(1).write.parquet("file:///home/hadoop/result/join3")
    //result2.write.jdbc(ohost,"TEST",oprop)
     resultsql.write.jdbc(ohost1,"TESTJOIN",oprop1)
    spark.stop()
  }
}