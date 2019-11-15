package com.spark.analyze

import java.io.{BufferedReader, InputStreamReader}
import java.util.{Calendar, Properties}
import java.util.zip.ZipInputStream

import org.apache.spark.input.PortableDataStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object UserAnalyze {
  def main(args: Array[String]): Unit = {
    //构建spark 上下文
    var home: String = System.getProperty("hadoop.home.dir")
    if (home == null) {
      println("null:" + home)
      home = System.getenv("HADOOP_HOME")
    }
    val sparkConf = new SparkConf().setAppName("UserAnalyze").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkConf).config("spark.sql.warehouse.dir", "F:/project/sparkproject/spark-warehouse")
      .getOrCreate()
    val sc = spark.sparkContext
    //从hdfs上加载数据
    val dataRDD = loadZipDataFromHdfs(sc)
    val distinctRDD = dataRDD.distinct()
    //把RDD 转成DataFrame
    val movieDF: DataFrame = spark.read.json(distinctRDD)
    movieDF.createOrReplaceTempView("Movie")

    //最近一个月的数据
    val testLastMonthMSql = "select * from Movie  where  time!=-1 and  datediff(now() ,FROM_UNIXTIME(time/1000,'yyyy-MM-dd') )<=30 "
    val oneMouthDF: DataFrame = spark.sql(testLastMonthMSql)
    oneMouthDF.createOrReplaceTempView("oneMonths")
    //saveToMysql(oneMouthDF, "oneMonthsTest")
    //
    //    //最近半年的数据
    val testhaldYears = "select * from Movie  where  time!=-1 and  datediff(now() ,FROM_UNIXTIME(time/1000,'yyyy-MM-dd') )<=180 "
    val haldYears: DataFrame = spark.sql(testhaldYears)
    haldYears.createOrReplaceTempView("halfYears")
    // saveToMysql(oneMouthDF, "halfYearsTest")


    // 一个月 影片的排名
    val oneMoviesql = "select name,cid,count(*) as num from oneMonths group by name,cid having cid=1 and trim(name) !='' order by num desc"
    val oneMovieTop10DF: DataFrame = spark.sql(oneMoviesql)
    oneMovieTop10DF.show(10)
    saveToMysql(oneMovieTop10DF.limit(10), "TopNMovie_one")
    //
    //
    val halfYearMoviesql = "select name,cid,count(*) as num from halfYears group by name,cid having cid=1 and trim(name) !='' order by num desc"
    val halfYearMovieTop10DF: DataFrame = spark.sql(halfYearMoviesql)
    halfYearMovieTop10DF.show(10)
    saveToMysql(halfYearMovieTop10DF.limit(10), "TopNMovie_half")
    //
    //
    val testMoviesql = "select name,cid,count(*) as num from movie  group by name,cid having cid=1 and trim(name) != '' order by num desc"
    val testMovieTop10DF: DataFrame = spark.sql(testMoviesql)
    saveToMysql(testMovieTop10DF.limit(10), "TopNMovie")
    //
    //
    //    //影片类型喜好
    //    //最近一个月
    val oneLikeTypeSql = "select cid,count(*) as num from oneMonths  group by cid having cid>0 and cid!=515   order by num desc"
    val oneMoviewTypeCount: DataFrame = spark.sql(oneLikeTypeSql)
    saveToMysql(oneMoviewTypeCount.filter(_.getAs("cid") != 517).limit(10), "MovieTypeCount_one")
    //
    //    //最近半年
    val halfLikeTypeSql = "select cid,count(*) as num from halfYears  group by cid having cid>0 and cid!=515   order by num desc"
    val halfMoviewTypeCount: DataFrame = spark.sql(halfLikeTypeSql)
    saveToMysql(halfMoviewTypeCount.filter(_.getAs("cid") != 517).limit(10), "MovieTypeCount_half")
    //
    //    //最近一年
    val textLikeTypeSql = "select cid,count(*) as num from movie  group by cid having cid>0 and cid!=515  order by num desc"
    val moviewTypeCount: DataFrame = spark.sql(textLikeTypeSql)
    saveToMysql(moviewTypeCount.filter((_.getAs("cid") != 517)).limit(10), "MovieTypeCount")


    //进行统计各个省的用户

    //    //最近一个月
    val onePerProvinceUserSql = "select distinct  uuid,province  from oneMonths group by uuid,province"
    val oneUserProvinceDF = spark.sql(onePerProvinceUserSql);
    oneUserProvinceDF.createOrReplaceTempView("userarea")
    val oneCountSql = "select province, count(*) as num from userarea group by province having trim(province) !=''  order by num desc"
    val oneUserProvinceCountDF: DataFrame = spark.sql(oneCountSql)
    saveToMysql(oneUserProvinceCountDF, "TopNProvince_one")
    //    //最近半年
    val halfPerProvinceUserSql = "select distinct  uuid,province  from halfYears group by uuid,province"
    val halfUserProvinceDF = spark.sql(halfPerProvinceUserSql);
    halfUserProvinceDF.createOrReplaceTempView("userarea")
    val halfCountSql = "select province, count(*) as num from userarea group by province  having trim(province) !='' order by num desc"
    val halfUserProvinceCountDF: DataFrame = spark.sql(halfCountSql)
    saveToMysql(halfUserProvinceCountDF, "TopNProvince_half")

    val textPerProvinceUserSql = "select distinct  uuid,province  from Movie group by uuid,province"
    val userProvinceDF = spark.sql(textPerProvinceUserSql);
    userProvinceDF.createOrReplaceTempView("userarea")
    val textCountSql = "select province, count(*) as num from userarea group by province  having trim(province) !=''  order by num desc"
    val userProvinceCountDF: DataFrame = spark.sql(textCountSql)
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
    val baseDir = "hdfs://192.168.10.123:8020/movie"
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
