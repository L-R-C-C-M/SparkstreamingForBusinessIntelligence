package org.example.Streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.example.Utils.JDBCUtil
import org.joda.time.DateTime

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Date
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

object ToMysql {

  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    // 2. 创建 StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // 3. 定义 Kafka 参数
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->
        "172.16.224.239:9092,172.16.224.240:9092,172.16.224.238:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "topic_log",
      "key.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer"
    )
    // 4. 读取 Kafka 数据创建 DStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Set("topic_log"), kafkaPara))

    val ClicksData = kafkaDStream.map(
      kafkaData => {
        val data = kafkaData.value()
        val pattern: Regex = """.*"userID":"([^"]+).*"pos":"([^"]+).*"neg":"([^"]+).*"start":"([^"]+).*"dwelltime_pos":"([^"]+)""".r

        val matches: Option[Regex.Match] = pattern.findFirstMatchIn(data)

        val posArray: Array[String] = matches match {
          case Some(m) => m.group(2).split(" ")
          case None =>  Array.empty[String]
        }

        val startTime: String = matches match {
          case Some(m) => m.group(4)
          case None => ""
        }

        val dwelltimePosArray: Array[String] = matches match {
          case Some(m) => m.group(5).split(" ")
          case None => Array.empty[String]
        }

        val userID: String = matches match {
          case Some(m) => m.group(1)
          case None => ""
        }

        val clicks: Seq[Clicks] = posArray.zipWithIndex.map { case (pos, index) =>
          val time = startTime + dwelltimePosArray(index)
          // 定义时间字符串的格式
          val formatter = DateTimeFormatter.ofPattern("M/d/yyyy h:mm:ss a")
          val formatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
          // 将时间字符串解析为 LocalDateTime 对象
          val dateTime = LocalDateTime.parse(startTime, formatter)

          // 添加秒数
          val addseconds = dwelltimePosArray.take(index).map(_.toInt).sum
          val updatedDateTime = dateTime.plusSeconds(addseconds)
          // 将更新后的时间转换回字符串
          val updatedTimeString = updatedDateTime.format(formatter2)
          Clicks(userID, pos, updatedTimeString, "category")
        }
        clicks


      }
    )

    // 周期性拿到RDD
    ClicksData.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val conn: Connection = JDBCUtil.getConnection
        val getCategoryCommand: PreparedStatement = conn.prepareStatement("SELECT Category FROM News WHERE News_ID=?")
//        val getDailyCommand: PreparedStatement = conn.prepareStatement("SELECT Day FROM DailyClick WHERE ClickNews=?")
        val command: PreparedStatement = conn.prepareStatement("INSERT INTO Click_Test (UserId, ClickNews, ClickTime,Category) VALUES (?, ?, ?,?)")
        partition.foreach(data => {
          data.foreach(click => {
            getCategoryCommand.setString(1,click.clickNews)
//            getDailyCommand.setString(1,click.clickNews)
            var category: String = ""
//            var day: String = ""
            val resultSet: ResultSet = getCategoryCommand.executeQuery()
//            val resultSet2: ResultSet = getCategoryCommand.executeQuery()
            if (resultSet.next()) {
              category = resultSet.getString(1) // 或 resultSet.getString("Category")
              // 在这里使用获取到的 category 值进行后续处理
//              println("Category: " + category)
            }
            resultSet.close()
//            if (resultSet2.next()) {
//              day = resultSet2.getString(1) // 或 resultSet.getString("Category")
//              // 在这里使用获取到的 category 值进行后续处理
////              println("Day: " + day)
//            }
//            resultSet2.close()

            command.setString(1, click.userId)
            command.setString(2, click.clickNews)
            command.setString(3, click.clickTime)
            command.setString(4, category)
            command.addBatch()
          })
        })
        command.executeBatch()
        command.close()
        conn.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  case class Clicks(userId: String, clickNews: String, clickTime: String,category: String)
}
