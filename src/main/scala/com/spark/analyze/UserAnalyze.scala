package com.spark.analyze

import java.io.{BufferedReader, InputStreamReader}
import java.util.Properties
import java.util.zip.ZipInputStream

import com.spark.mode.DataModel.TopNMovie
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._


/**
  * 求前电影前10的访问量
  */
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
    import spark.implicits._
    //获取name数据进行统计
    val movieNameAndOne: RDD[(String, Int)] = movieDF.map(x => (x.getAs("name").toString, 1)).rdd
  //  movieNameAndOne.map(x => TopNMovie(x._1, x._2))
    //过滤空值，进行累加
    val movieNameTotal: RDD[(String, Int)] = movieNameAndOne.filter(!_._1.equals("")).reduceByKey(_ + _)
    //进行倒排序
    val sortMovieName: RDD[(String, Int)] = movieNameTotal.sortBy(_._2, false)
    val sortMovieNameDF = sortMovieName.toDF("movie_name","count")
    sortMovieNameDF.createOrReplaceTempView("TopNMovie")
    val sortMovieTop10: Dataset[Row] = sortMovieNameDF.limit(10)
    saveToMysql(sortMovieTop10, "TopNMovie")


    //进行统计用户分布的各个省
    val movieProvinceAndOne: RDD[(String, Int)] = movieDF.map(x => (x.getAs("province").toString, 1)).rdd

    val movieProvinceTotal: RDD[(String, Int)] = movieProvinceAndOne.filter(!_._1.equals("")).reduceByKey(_ + _)

    val movieProvinceDF = movieProvinceTotal.toDF("province_name","count")

    movieProvinceDF.createOrReplaceTempView("TopNProvince")

    saveToMysql(movieProvinceDF, "TopNProvince")

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
