package com.junjun.etl

import java.io.{PrintWriter, StringWriter}
import java.util.Properties
import com.alibaba.fastjson.JSON
import org.slf4j.LoggerFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by Administrator on 2018/1/4.
  * 本程序为1.0的程序，也就这一个程序
  * 实现步骤：加载数据->变成临时表->sql查询形成DataFrame->写入对应的数据库中，全程的参数都是用json字符串传递的
  */
object lemon01 {
   val logger = LoggerFactory.getLogger(this.getClass)
   def main(args: Array[String]) {
     var sc:SparkContext = null
     try{
       //处理JSON 抽取传入参数
       logger.info("************************ args: " + args(0))
       val json=JSON.parseObject(args(0))
       val conf = new SparkConf()
         .setAppName("sparkProject")
       logger.info("开始初始化sc！")
       sc = new SparkContext(conf)
       val sqlContext = new SQLContext(sc)
       logger.info("初始化完成，输入参数为：" + args(0))
       //获取传入的数据源地址
       val inputString=json.getString("path")
       //得到数据表名列表
       val tableList=json.getJSONObject("tableList").keySet()
       //遍历数据表名，根据传入的路径和表名组成文件的具体路径，提取数据，然后生成临时表，临时表名就是遍历的表名
       for (x <- tableList){
         logger.info("************* tablelist:" + x)
         val devRdd=sqlContext.read.format("parquet").load(inputString + "/" + x + ".parquet")
         devRdd.registerTempTable(x)
       }
       //输出数据的表名
       val outputTable=json.getString("outTable")
       logger.info("************************ outputTable: " + outputTable)
       //传入的SQL语句
       val sql = json.getString("outSql")
       logger.info("************************ sql: " + sql)
       val resDF=sqlContext.sql(sql)
       logger.info("************************ rsSize: " + resDF.count())
       logger.info("************************ start to write data!!!")
       //创建Properties存储数据库相关属性
       val prop = new Properties()
       prop.put("user", "xdbdrs")
       prop.put("password", "chenggang123")
       //将数据追加到数据库
       resDF.write.mode("append").jdbc("jdbc:mysql://172.16.1.123:3306/xdbdrs?useUnicode=true&characterEncoding=utf-8", outputTable, prop)
       logger.info("****************数据库写入完成！****************************")
     } catch {
       case ex:Exception => {
         logger.info("********************* Error catched!!!")
         val sw:StringWriter = new StringWriter()
         val pw:PrintWriter = new PrintWriter(sw)
         ex.printStackTrace(pw)
         logger.info(sw.toString)
         logger.info("********************* Error catched!!!")
         throw ex;
       }
     }finally{
       //停止SparkContext
       sc.stop()
     }
   }
 }

