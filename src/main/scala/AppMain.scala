package com.xuehai.utils

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer



object QingzhouMain extends Constants{

  def main(args: Array[String]) {
    taskmain()
  }
  def taskmain(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //env.enableCheckpointing(20 * 1000)//开启checkPoint，并且每分钟做一次checkPoint保存
    //env.setStateBackend(new FsStateBackend(checkPointPath))
    //env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    //env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.of(1, TimeUnit.MINUTES)))//设置重启策略，job失败后，每隔10分钟重启一次，尝试重启100次
    val quUserInfoSql="select a.iUserId,a.iSchoolId,a.sUserName,a.iUserType,b.sSchoolName,b.scountyname  from \nxh_user_service.XHSys_User a\nLEFT JOIN \nxh_user_service.XHSchool_Info b \non  a.iSchoolId=b.ischoolid and b.bdelete=0 and b.istatus in (1,2)"
    var emptyMap = new mutable.HashMap[Int,JSON]()
    val results: ResultSet = MysqlUtils.select(quUserInfoSql)


    while(results.next()){
      val  json = JSON.parseObject("{}")
      val user_id=results.getInt(1)
      json.put("user_id",results.getInt(1))
      json.put("school_id",results.getString(2))
      json.put("user_name",results.getString(3))
      json.put("user_type",results.getInt(4))
      json.put("school_name",results.getString(5))
      json.put("city",results.getString(6))
      emptyMap+=(user_id -> json)
    }
    // 用相对路径定义数据源
   // val resource = getClass.getResource("/hello.txt")
   //  val dataStream = env.readTextFile(resource.getPath)
    val kafkaConsumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema(), props)
    env.addSource(kafkaConsumer)

      .map(x => {
        try{
          //println(x)
          val json=	JSON.parseObject(x)
          val obj: JSONObject = JSON.parseObject(x)
          val sql_type=json.getString("type")
          val table=json.getString("table")
          obj.put("type",sql_type)
          obj.put("table",table)
          val data=JSON.parseArray(json.getString("data")).get(0).asInstanceOf[JSONObject]
          if (table=="coupon"){
            val	userid=data.getString("user_id")
            val	no=data.getString("no")
            val	batch_no=data.getString("batch_no")
            val	batch_id=data.getString("batch_id")
            val	modified_date=data.getString("modified_date")
            val	status_changed_date=data.getString("status_changed_date")
            val	distribution_channel=data.getString("distribution_channel")
            val	created_date=data.getString("created_date")
            val	expire_start=data.getString("expire_start")
            val	expire_end=data.getString("expire_end")
            val	deleted=data.getString("deleted")
            val	status=data.getString("status")
            obj.put("userid",userid)
            obj.put("no",no)
            obj.put("batch_no",batch_no)
            obj.put("batch_id",batch_id)
            obj.put("modified_date",modified_date)
            obj.put("status_changed_date",status_changed_date)
            obj.put("distribution_channel",distribution_channel)
            obj.put("created_date",created_date)
            obj.put("expire_start",expire_start)
            obj.put("expire_end",expire_end)
            obj.put("deleted",deleted)
            obj.put("status", status)
          }
          if (table=="coupon_use_record"){
            val order_no=data.getString("order_no")
            val coupon_no=data.getString("coupon_no")
            val used_date=data.getString("created_date")
            obj.put("order_no",order_no)
            obj.put("coupon_no",coupon_no)
            obj.put("used_date",used_date)
          }
        obj
        }
        catch {
          case e: Exception => {
            //Utils.dingDingRobot("all", "错题本实时数据异常：%s, %s".format(e, x))
            println(("数据异常：%s, \\r\\n %s".format(e, x)))
          //  coupon_history_info("","","","","","","","","","","",0)
            null
          }
        }
      })

     .addSink(new  MySqlSink2())

     //.print()

    env.execute("coupon job")
  }
}









class MySqlSink2() extends RichSinkFunction[JSONObject] with Constants {
  // 定义sql连接、预编译器
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var insertStmtStu: PreparedStatement = _
  var result: ResultSet = null
  var updateStmt: PreparedStatement = _
  var status = ""

  import org.apache.commons.dbcp2.BasicDataSource

  var dataSource: BasicDataSource = null
  var Map = new mutable.HashMap[Int, String]()
  val arr = ArrayBuffer[String]()
  var student_info = ""
  var useridString = ""


  // 初始化，创建连接和预编译语句
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    try {
      import org.apache.commons.dbcp2.BasicDataSource
      dataSource = new BasicDataSource
      conn = getConnection(dataSource)
      conn.setAutoCommit(false) // 开始事务
      insertStmt = conn.prepareStatement("INSERT INTO `baifen_stats_coupon` ( `no`, `batch_id`, `batch_no`, `status`, `status_changed_date`, `deleted`, `distribution_channel`, `expire_end`, `expire_start`, `user_id`, `user_name`, `user_grade_id`, `user_grade_name`, `user_school_id`, `user_school_name`, `obtain_time`) VALUES (?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?)")
      updateStmt = conn.prepareStatement("update baifen_stats_coupon set use_time=?,order_no=? where no =? and status =50")

    }
    catch {
      case e: Exception => {
        println("云mysql连接失败")
      }
    }
  }

  // 调用连接，执行sql
  override def invoke(values: JSONObject, context: SinkFunction.Context[_]): Unit = {

    try {
    if (values.getString("table") == "coupon") {
      val quUserInfoSql = "select student_id,school_id,school_name,gradeid,grade_name,student_name from fact_student_info_distinct_daily where student_id =" + values.getString("userid");
      val results: ResultSet = MysqlUtils.select(quUserInfoSql)
      while (results.next()) {
        val userid = results.getInt(1)
        val school_id = results.getInt(2)
        val school_name = results.getString(3)
        val gradeid = results.getInt(4)
        val grade_name = results.getString(5)
        val student_name = results.getString(6)
        student_info = userid + "#" + school_id + "#" + school_name + "#" + gradeid + "#" + grade_name + "#" + student_name
        Map += (userid -> student_info)
      }

        insertStmt.setString(1, values.getString("no"))
        insertStmt.setInt(2, values.getString("batch_id").toInt)
        insertStmt.setString(3, values.getString("batch_no"))
        insertStmt.setInt(4, values.getString("status").toInt)
        insertStmt.setString(5, values.getString("status_changed_date"))
        insertStmt.setInt(6, values.getString("deleted").toInt)
        insertStmt.setString(7, values.getString("distribution_channel"))
        insertStmt.setString(8, values.getString("expire_end"))
        insertStmt.setString(9, values.getString("expire_start"))
        insertStmt.setInt(10, values.getString("userid").toInt)
        insertStmt.setString(11, Map.get(values.getString("userid").toInt).get.split("#")(5))
        insertStmt.setInt(12, Map.get(values.getString("userid").toInt).get.split("#")(3).toInt)
        insertStmt.setString(13, Map.get(values.getString("userid").toInt).get.split("#")(4))
        insertStmt.setInt(14, Map.get(values.getString("userid").toInt).get.split("#")(1).toInt)
        insertStmt.setString(15, Map.get(values.getString("userid").toInt).get.split("#")(2))
        insertStmt.setString(16, values.getString("created_date"))
      insertStmt.addBatch()
    }

      if(values.getString("table") == "coupon_use_record"){
        updateStmt.setString(1, values.getString("used_date"))
        updateStmt.setString(2, values.getString("order_no"))
        updateStmt.setString(3, values.getString("coupon_no"))
        updateStmt.addBatch()
      }
    Map.clear()
    student_info = ""

    val count1 = insertStmt.executeBatch //批量后执行
    val count2=updateStmt.executeBatch()

    conn.commit
    }catch
  {
    case e: Exception => {
      log.error("数据异常：%s, \\r\\n %s".format(e, values))
    }
  }
}


  // 关闭时做清理工作
  override def close(): Unit = {
    try {
     // insertStmtStu.close()
      insertStmt.close()
      updateStmt.close()
      conn.close()
     //  println("云mysql关闭成功")
    } catch {
      case e: Exception => {
     //   println("云mysql关闭失败")
      }
    }

  }

  def getConnection(dataSource: BasicDataSource):Connection= {
    dataSource.setDriverClassName("com.mysql.jdbc.Driver")

    dataSource.setUrl(Url) //test为数据库名

    dataSource.setUsername(User) //数据库用户名

    dataSource.setPassword(Password) //数据库密码

    //设置连接池的一些参数
    dataSource.setInitialSize(10)
    dataSource.setMaxTotal(1004)
    dataSource.setMinIdle(10)
    var con: Connection = null
    try {
      con =dataSource.getConnection
      con
    } catch {
      case e: Exception =>
        System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage)
        con
    }

  }



}