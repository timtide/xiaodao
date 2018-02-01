package com.junjun.etl

import java.sql.DriverManager

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory


/**
  * Created by Administrator on 2017/10/11.
 * 本程序为2.0的程序，也就这一个程序
  */
object midetl {
   val logger = LoggerFactory.getLogger(this.getClass)
   def main(args: Array[String]) {
     //处理JSON 抽取传入参数
     val str2 = args(0)
     val json = JSON.parseObject(str2)
     val actionId = json.getString("actionId")
     // 访问本地MySQL服务器，通过3306端口访问mysql数据库
     val url = json.getString("url")
     //用户名
     val username = json.getString("username")
     //密码
     val password = json.getString("password")
     val conf = new SparkConf()
       .setAppName("sparkProject" + actionId)
     val sc = new SparkContext(conf)
     val sqlContext = new SQLContext(sc)
     try {
       //获取传入的数据源地址
       val inputString = json.getString("path")
       //得到数据表名列表
       val tableList = json.getJSONObject("tableList").keySet()
       //遍历数据表名，根据传入的路径和表名组成文件的具体路径，提取数据，然后生成临时表，临时表名就是遍历的表名
       for (x <- tableList) {
         val devRdd = sqlContext.read.format("parquet").load(inputString + "/" + x + ".parquet")
         devRdd.registerTempTable(x)
       }
       //输出数据的表名
       val midTableName = json.getString("midTableName")
       //传入的SQL语句
       val sql = json.getString("outSql")
       val resDF = sqlContext.sql(sql)
       val cols = resDF.columns
       val columnString = cols.mkString(",")
       val len = cols.length
       Class.forName("com.mysql.jdbc.Driver")
       //得到连接
       val connection = DriverManager.getConnection(url, username, password)
       val baseSql = "INSERT INTO" + midTableName + "(" + columnString + ") VALUES "
       try {
         val rows = resDF.map(lines => lines.mkString(",").split(",")).collect()
         val batchSize = 1000
         var count = 0
         var batchInsertSql = baseSql
         for (row <- rows) {
           if (count > 0) {
             batchInsertSql += ","
           }
           batchInsertSql += "("
           for (i <- 0 to len - 1) {
             if (i > 0) {
               batchInsertSql += ","
             }
             batchInsertSql += "'" + row(i) + "'"
           }
           batchInsertSql += ")"
           count += 1
           if (count == batchSize) {
             // executeUpdate()
             connection.createStatement().executeUpdate(batchInsertSql)
             count = 0
             batchInsertSql = baseSql
           }
         }
         if (count > 0) {
           connection.createStatement().executeUpdate(batchInsertSql)
         }
       } catch {
         case ex: Exception => {
           println(ex.getMessage)
           throw ex
         }
       } finally {
         //停止SparkContext
         sc.stop()
       }
     }
   }
 }
