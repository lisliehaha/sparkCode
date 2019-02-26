package com.mfw.sparksql

import com.mfw.utils.{FileUtils, GetSparkSession}
import org.apache.spark.sql.SaveMode

/**
  * @program: sparkCode
  * @description: ${description}
  * @author: Liusengen
  * @create: 2019-02-25 11:26
  **/
object Semi_im_im_consult {
  def main(args: Array[String]): Unit = {
    if(args.length!=2){
      println("参数应为2")
      sys.exit()
    }
    val spark = GetSparkSession.getsparkSession
    val sqltext=FileUtils.inputToString("/insert_semi_im_im_consult.sql")
      .replace("'{dt1}'",args(0))
      .replace("'{dt2}'",args(1))
    spark.sql("set spark.sql.shuffle.partitions=800")
    val df=spark.sql(sqltext)

    df.coalesce(1)
      .write.format("hive")
      .mode(SaveMode.Overwrite)
      .partitionBy("dt")
      .saveAsTable("tmpdb.tmp_im_consult")
    spark.stop()

  }
}
