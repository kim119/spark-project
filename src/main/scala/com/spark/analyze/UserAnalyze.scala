package com.spark.analyze

import java.io.{BufferedReader, InputStreamReader}
import java.util.Properties
import java.util.zip.ZipInputStream
import org.apache.spark.input.PortableDataStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._


object UserAnalyze {
  def main(args: Array[String]): Unit = {
    //构建spark 上下文
    var home: String = System.getProperty("hadoop.home.dir")
    if (home == null) {
      println("null:" + home)
      home = System.getenv("HADOOP_HOME")
    }
    val sparkConf = new SparkConf().setAppName("UserAnalyze").setMaster("local[2]")
    val spark = SparkSession.builder().config(sparkConf)
      .getOrCreate()
    val sc = spark.sparkContext
    //从hdfs上加载数据
    val dataRDD = loadZipDataFromHdfs(sc)
    val distinctRDD = dataRDD.distinct()
    //把RDD 转成DataFrame
    val movieDF: DataFrame = spark.read.json(distinctRDD)
    movieDF.createOrReplaceTempView("Movie")
    //影片的排名
    var textMoviesql = "select name,cid,count(*) as num from movie  group by name,cid having cid=1 and trim(name) != '' order by num desc"
    val movieTop10DF: DataFrame = spark.sql(textMoviesql)
    // movieTop10DF.show(10)
    saveToMysql(movieTop10DF.limit(10), "TopNMovie")

    //影片类型喜好
    var textLikeTypeSql = "select cid,count(*) as num from movie  group by cid having cid>0 and cid!=515  order by num desc"
    val moviewTypeCount: DataFrame = spark.sql(textLikeTypeSql)
    saveToMysql(moviewTypeCount.limit(10), "MovieTypeCount")


    //进行统计各个省的用户
    val textPerProvinceUserSql = "select distinct  uuid,province  from Movie group by uuid,province"
    val userProvinceDF = spark.sql(textPerProvinceUserSql);
    userProvinceDF.createOrReplaceTempView("userarea")
    val textCountSql = "select province, count(*) as num from userarea group by province  order by num desc"
    val userProvinceCountDF: DataFrame = spark.sql(textCountSql)
    // userProvinceCountDF.show(20)
    saveToMysql(userProvinceCountDF, "TopNProvince")
    spark.stop()


  }

  /**
   * 保存到mysql
   *
   * @param dtData
   */
  private def saveToMysql(dtData: DataFrame, mysqlName: String) = {

    // 9.把结果保存在mysql表中
    val prop: Properties = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "123456")
    dtData.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://192.168.1.126:3306/user?serverTimezone=UTC", mysqlName, prop)
  }


  /**
   * 从hdfs 上加载数据 zip数据
   *
   * @param sc
   */
  def loadZipDataFromHdfs(sc: SparkContext) = {
    //hdfs地址
    val baseDir = "hdfs://192.168.1.123:8020/movie"
    //读取hdfs 上的zip
    val dataRDD = sc.binaryFiles(s"$baseDir/*/*.zip", 40)
      .flatMap {
        case (name: String, content: PortableDataStream) => {
          val zis = new ZipInputStream(content.open())
          Stream.continually(zis.getNextEntry)
            .takeWhile(_ != null)
            .flatMap { _ =>
              val br = new BufferedReader(new InputStreamReader(zis))
              Stream.continually(br.readLine()).takeWhile(_ != null)
            }
        }
      }
    //进行输出
    dataRDD

  }

}
