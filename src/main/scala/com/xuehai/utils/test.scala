//package com.xuehai.utils
//
//import java.sql.ResultSet
//import java.text.SimpleDateFormat
//import java.util.{Date, Locale}
//
//import com.alibaba.fastjson.JSON
//import com.xuehai.utils.MysqlUtils.{conn, getMysqlConnection}
//
//import scala.collection.mutable
//
//object test {
//  def main(args: Array[String]): Unit = {
//    var aa="2020-09-15 15:32:11"
//    val str = aa.split(" ")(0)
//    println(str)
//
//    val collectMap = new mutable.HashMap[String,Int]()
//    for(value <-input) {
//
//      if (collectMap.contains(value.school_id)) { //若map中已经存在
//        val maybeInt: Option[Int] = collectMap.get(value.school_id)
//
//        collectMap.put(value.school_id, maybeInt.get+1)
//      }
//      else collectMap.put(value.school_id, 1)
//
//    }
//  }
//}
