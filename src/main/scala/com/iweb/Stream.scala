package com.iweb
import java.util.Properties

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object Stream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("stream")
    val ssc = new StreamingContext(conf,Seconds(3))
    ssc.sparkContext.setLogLevel("OFF")
    //    KafkaUtils.
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "bigdata1:9092,bigdata2:9092,bigdata3:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "mor1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    val topics = Array("mor1")
    val stream = KafkaUtils.createDirectStream[String,String](
      ssc,
      PreferConsistent,
      Subscribe[String,String](topics,kafkaParams)
    )
    val spark=SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    //连接参数
    val prop = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","123456")
    val r1=stream.map(p=>p.value()).map(TopicUtils.parseMessage)
      .map(_.split("\t"))
      .filter(e=>{e(3).equals("11")&&e(8).contains("药")})
      .map(e=>{(e(7),e(e.length-1))})
      .groupByKeyAndWindow(Seconds(9),Seconds(3))
      .map(e=>{(e._1,TopicUtils.avgUser(e._2),e._2.toString())})
      .mapPartitions(x=>{
              val result = ListBuffer[GameData]()
              while (x.hasNext){
                val b = x.next()
                result+=GameData(Some(b._1),Some(b._2),Some(b._3))
              }
              println(result.toBuffer)
              result.iterator
            }).foreachRDD(rdd=>{
              rdd.toDF()
                .write
                .mode("append")
                .jdbc("jdbc:mysql://iwebdb:3306?characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai", "sparksql.gamedata", prop)
            })




//        .map(_.split("\t"))
//        .filter(e=>{e(3).equals("11")&&e(8).contains("药")})
//        .map(e=>{(e(7),e(e.length-1))})
//        .groupByKeyAndWindow(Seconds(9),Seconds(3))
//        .map(e=>{(e._1,TopicUtils.avgUser(e._2),e._2.toString())})
//        .mapPartitions(x=>{
//          val result = ListBuffer[GameData]()
//          while (x.hasNext){
//            val b = x.next()
//            result+=GameData(Some(b._1),Some(b._2),Some(b._3))
//          }
//          println(result.toBuffer)
//          result.iterator
//        }).foreachRDD(rdd=>{
//      rdd.toDF()
//        .write
//        .mode("append")
//        .jdbc("jdbc:mysql://iwebdb:3306?characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai", "sparksql.gamedata", prop)
//    })
    ssc.start()
    ssc.awaitTermination()

  }
}
