package com.mfw.sparksql

import com.mfw.utils.{FileUtils, GetSparkSession}
import org.apache.spark.sql.SparkSession

object Spark_hive_sql {

  def main(args: Array[String]): Unit = {
    val spark = GetSparkSession.getsparkSession
    val sqltext=FileUtils.inputToString("/test.sql")
      println(sqltext)
      spark.sql("set spark.sql.shuffle.partitions=800")
      spark.sql(sqltext).show()
      //spark.table("tmpdb.fact_flow_push_first_launch").repartition(1).persist()
  }

}
