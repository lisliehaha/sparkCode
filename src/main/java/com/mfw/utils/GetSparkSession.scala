package com.mfw.utils

import org.apache.spark.sql.SparkSession

/**
  * @program: sparkCode
  * @description: ${description}
  * @author: Liusengen
  * @create: 2019-02-25 11:25
  **/
object GetSparkSession {
  def getsparkSession:SparkSession={
    val spark = SparkSession
      .builder()
      .appName("Spark SQL example")
      .config("spark.sql.warehouse.dir", "hdfs://mfwCluster/user/hive/warehouse/")
      .enableHiveSupport()
      .getOrCreate()
    spark
  }
}
