package com.mfw.utils

import java.io.{File, FileInputStream, PrintWriter}

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

  def replacTextContent(path1:String,oldstr:String,newstr:String):String={
    val lines:Iterator[String] = Source.fromFile(path1).getLines()
    val sb=new StringBuilder
    lines.foreach{line=>
      line.trim
      sb.append(line.replaceAll(oldstr,newstr))
      sb.append("\n")
    }
    sb.toString()
  }

  def main(args: Array[String]): Unit = {
    val str=FileUtils.inputToString("/test.sql")
   // println(str)
    val str2=FileUtils.replacTextContent("src/main/resources/test.sql","20190120","20190122")
    println(str2)
  }
}
