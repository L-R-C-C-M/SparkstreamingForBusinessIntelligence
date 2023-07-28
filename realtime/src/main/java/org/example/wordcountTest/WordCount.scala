package org.example.wordcountTest

import org.apache.hadoop.yarn.util.StringHelper._split
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
//    //1.初始化 Spark 配置信息
//    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")
//    //2.初始化 SparkStreamingContext
//    val ssc = new StreamingContext(sparkConf, Seconds(3))
//    //3.通过监控端口创建 DStream，读进来的数据为一行行
//    val lineStreams = ssc.socketTextStream("linux1", 9999)
//    //将每一行数据做切分，形成一个个单词
//    val wordStreams = lineStreams.flatMap(_.split(" "))
//    //将单词映射成元组（word,1）
//    val wordAndOneStreams = wordStreams.map((_, 1))
//    //将相同的单词次数做统计
//    val wordAndCountStreams = wordAndOneStreams.reduceByKey(_ + _)
//    //打印
//    wordAndCountStreams.print()
//    //启动 SparkStreamingContext
//    ssc.start()
//    ssc.awaitTermination()
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc= new SparkContext(sparkConf)

    var lines:RDD[String]=sc.textFile("datas")
    var words:RDD[String]= lines.flatMap(_.split(" "))
    var wordGroup:RDD[(String,Iterable[String])]=words.groupBy(word=>word)
    var wordToCount = wordGroup.map{
      case(word,list)=>{
        (word,list.size)
      }
    }
    val array:Array[(String,Int)]=wordToCount.collect()
    array.foreach(println)
    sc.stop()
  }
}
