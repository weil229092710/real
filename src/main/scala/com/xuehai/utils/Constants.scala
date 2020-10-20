package com.xuehai.utils

import java.util.Properties
import org.slf4j.{LoggerFactory, Logger}

/**
  * Created by root on 2019/11/13.
  */
trait Constants {

	val LOG = LoggerFactory.getLogger(this.getClass)

	/**
	  * kafka
	  */
	val brokerList = PropertiesUtil.getKey("brokerList")
	val topic = PropertiesUtil.getKey("topicName")
	val onlinetopic = PropertiesUtil.getKey("topicOnlineName")

	val groupId = PropertiesUtil.getKey("kafkaGroupId")
	val props = new Properties()
	props.put("bootstrap.servers", brokerList)
	props.put("auto.offset.reset", "latest")//earliest
	props.put("group.id", groupId)
	props.put("enable.auto.commit", "false")
	props.put("auto.commit.interval.ms", "1000")
	props.put("session.timeout.ms", "30000")
	props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
	props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

	/**
	  * checkPoint
	  */
	val checkPointPath = PropertiesUtil.getKey("checkPointPath")


	/**
		* mysql
		*/
	val mysqlHost = PropertiesUtil.getKey("mysql_host")
	val mysqlPort = PropertiesUtil.getKey("mysql_port")
	val mysqlUser = PropertiesUtil.getKey("mysql_user")
	val mysqlPassword = PropertiesUtil.getKey("mysql_password")
	val mysqlDB = PropertiesUtil.getKey("mysql_db")
	val mysqlUtilsUrl = "jdbc:mysql://%s:%s/%s?autoReconnect=true&characterEncoding=utf8".format(mysqlHost, mysqlPort, mysqlDB)



	/**
		* 云mysql
		*/
	val Host = PropertiesUtil.getKey("Host")
	val Port = PropertiesUtil.getKey("Port")
	val User = PropertiesUtil.getKey("User")
	val Password = PropertiesUtil.getKey("Password")
	val DB = PropertiesUtil.getKey("DB")
	val Url = "jdbc:mysql://%s:%s/%s?autoReconnect=true&characterEncoding=utf8".format(Host, Port, DB)





	/**
	  * log日志系统
	  */
	val log: Logger = LoggerFactory.getLogger(this.getClass)

	/**
	  * 钉钉机器人
	  */
	val DingDingUrl: String = PropertiesUtil.getKey("dingding_url")

	/**
	  * 任务名称
	  */
	val jobName = PropertiesUtil.getKey("jobName")
}
