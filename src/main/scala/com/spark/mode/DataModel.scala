package com.spark.mode

object DataModel {

  /**
   * 用户访问动作表
   *
   * @param uuid
   * @param adType   用户的ID
   * @param name     Session的ID
   * @param province 某个页面的ID
   *
   */
  case class Movie(uuid: String, cid: Int, adType: String, name: String, province: String)

  /**
   * 用户区域表
   */

  case class userarea(uuid: String, province: String, num: Long)

  /**
   *
   * @param movieName 电影名字
   * @param num       点击次数
   */
  case class TopNMovie(movieName: String, cid: Int, num: Long)

  /**
   * 各省的用户量
   *
   * @param province
   * @param num
   */
  case class TopNProvince(province: String, num: Long)

  /**
   * 各种电影类型的总数
   * @param uuid
   * @param movieName
   */
  case class MovieTypeCount(uuid:Int,movieName:String)
}
