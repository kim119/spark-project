package com.spark

import java.util.Properties
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 数据写入mysql
  */
// 定义样例类
case class User(entry1Id: String, country: String, city: String)

object DataToMysql {

  def main(args: Array[String]): Unit = {

    //创建SparkSession 对象
    val spark = SparkSession.builder().appName("DataToMysql").master("local[2]").getOrCreate()
    val sc = spark.sparkContext
    //读取数据
    val data_line: RDD[String] = sc.textFile("C:\\Users\\Administrator.SC-201811211111\\Desktop\\20181126\\20181126")

    // 3.切分每一行
    val arrRDD: RDD[Array[String]] = data_line.map(_.split(","))

    // 4.RDD关联样例类
    val teacherRDD: RDD[User] = arrRDD.map(x => User(x(19).split(":")(1).replace("\"",""), x(10).split(":")(1).replace("\"",""), x(8).split(":")(1).replace("\"","")))

    // 5.导入隐式转换
    import spark.implicits._
    val teacherDF: DataFrame = teacherRDD.toDF()

    // 7.将DataFrame注册成表
    teacherDF.createOrReplaceTempView("User")

    //查询所有数据
    val resultDF: DataFrame = spark.sql("select * from User where city='深圳'")

    resultDF.show()
    // 9.把结果保存在mysql表中
    val prop: Properties = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

    resultDF.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://192.168.133.101:3306/data", "user", prop)

  }

}
