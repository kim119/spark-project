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
  case class Movie(uuid: String, adType: String, name: String, province: String)

  /**
    *
    * @param movieName  电影名字
    * @param num 点击次数
    */
  case class TopNMovie(movieName:String, num:Long)

  case  class TopNProvince(province:String,num:Long)

}
