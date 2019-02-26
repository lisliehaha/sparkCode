package com.mfw.sparksql

import com.mfw.utils.{DateUtil, FileUtils, GetSparkSession}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}

import scala.reflect.io.File




/**
  * @program: sparkCode
  * @description: 合并小文件的类
  * @author: Liusengen
  * @create: 2019-02-25 16:15
  **/
object MergeSmallFiles {
//执行命令：
// ./spark-submit --master yarn --num-executors 200 --executor-memory 20g --executor-cores 8   --queue root.spark --class com.mfw.sparksql.MergeSmallFiles /home/penghao/liusengen/jars/mergesmallfile.jar tmpdb.tmp_im_consult 20190119 20190219 1
  def main(args: Array[String]): Unit = {
    val spark = GetSparkSession.getsparkSession

    val tablename=args(0)

    val dataDir = "hdfs://mfwCluster/user/liusengen/temptable/tmptb"
    if (args.length==3){//长度等于3，默认到今天
      val now = DateUtil.getNowDate

      spark.sql("select * from " + tablename + " where dt >= " + args(1) + " and dt<" + now)
           .coalesce(args(2).toInt)
           .write.mode(SaveMode.Overwrite).parquet(dataDir)
      val par= spark.read.parquet(dataDir)
      par.write
         .format("hive")
         .mode(SaveMode.Overwrite)
         .partitionBy("dt")
         .saveAsTable(tablename)
      //FileUtils.deleteFile(dataDir)
      spark.stop()
    }else if(args.length==4){
      spark.sql("select * from " + tablename + " where dt >= " + args(1) + " and dt<=" + args(2))
        .coalesce(args(3).toInt)
        .write.mode(SaveMode.Overwrite).parquet(dataDir)

      val par=spark.read.parquet(dataDir)
      par.write.format("hive")
         .mode(SaveMode.Overwrite)
         .partitionBy("dt")
         .saveAsTable(tablename)
     // FileUtils.deleteFile(dataDir)
      spark.stop()
    }else {
      println("参数异常：参数个数应为3或4")
    }



  }
}
