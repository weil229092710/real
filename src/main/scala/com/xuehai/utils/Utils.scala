package com.xuehai.utils

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import com.dingtalk.api.DefaultDingTalkClient
import com.dingtalk.api.request.OapiRobotSendRequest

import scala.collection.JavaConverters._

/**
  * Created by Administrator on 2019/5/23 0023.
  */
object Utils extends Constants{

    /**
      * 钉钉机器人消息推送
      *
      * @param user "all"-@所有人，"18810314189,13724612033"-@指定的人，以逗号分割
      * @param massage 推送的消息
      */
    def dingDingRobot(user: String, massage: String): Unit ={
        val text = new OapiRobotSendRequest.Text()
        text.setContent(massage)

        val at = new OapiRobotSendRequest.At()
        if(user=="all"){
            at.setIsAtAll("true")
        }else{
            at.setAtMobiles(user.split(",").toList.asJava)
        }

        val request = new OapiRobotSendRequest()
        request.setMsgtype("text")
        request.setText(text)
        request.setAt(at)

        val client = new DefaultDingTalkClient(DingDingUrl)
        client.execute(request)
    }

    /**
      * 毫秒数生产日期和小时，例如1559615598000-》(20190604, 10)
      *
      * @param x long
      * @return (day, hour)
      */
    def str2Day(x: String): String ={
        val date = new Date(x.toLong)
        val dateFormat = new SimpleDateFormat("yyyyMMdd")
        val dateStr = dateFormat.format(date).split(" ")

        dateStr(0)
    }

    def toLoclTime(time: String): String ={
        import java.text.SimpleDateFormat
        val format = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.ENGLISH)
        val format1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
        format1.format(format.parse(time))
    }

    def main(args: Array[String]) {
        val str = str2Day("1559615598000")

        println(str)
    }
}
