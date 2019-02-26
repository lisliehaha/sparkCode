package com.mfw.utils

import java.io.{File, FileInputStream, PrintWriter}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

import scala.io.Source

object FileUtils {
  //用于将文件读取成字符串
  def inputToString(file: String): String = {
    //获取resource里的资源文件
    val in=this.getClass.getResourceAsStream(file)
    val lines: Iterator[String] = Source.fromInputStream(in, "utf-8").getLines()
    val sb=new StringBuilder
    lines.foreach{ line=>
      line.trim
      sb.append(line+"\n")
    }
    in.close()
    sb.toString()
  }
  def deleteFile(path: String): Boolean ={
    val sparkContext = SparkContext.getOrCreate()
    val hadoopConf = sparkContext.hadoopConfiguration
    val hdfs=FileSystem.get(hadoopConf)
    val path2=new Path(path)
    if(hdfs.exists(path2)) {
      hdfs.delete(path2, false)
    }else{
      false
    }
    }


  def main(args: Array[String]): Unit = {
    val str=FileUtils.inputToString("/test.sql")
  }
}
