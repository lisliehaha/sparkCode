package com.mfw.sparksql

import com.mfw.utils.FileUtils
import org.apache.spark.sql.SparkSession

object Spark_hive_sql {
  def getsparkSession:SparkSession={
    val spark = SparkSession
      .builder()
      .appName("Spark SQL example")
      .config("spark.sql.warehouse.dir", "hdfs://mfwCluster/user/hive/warehouse/")
      .enableHiveSupport()
      .getOrCreate()
    spark
  }
  def main(args: Array[String]): Unit = {
    val spark = getsparkSession
    val sqltext=FileUtils.inputToString("/test.sql")
      println(sqltext)
      spark.sql("set spark.sql.shuffle.partitions=800")
      spark.sql(sqltext).show()
    //val aDF =spark.table("tmpdb.fact_flow_push_first_launch").repartition(1).persist()
  }

}
