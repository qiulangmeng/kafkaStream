package com.iweb

import java.text.SimpleDateFormat

import scala.util.parsing.json.JSON

object TopicUtils {
private val format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
def parseMessage(jsonStr:String):String={
  val json= JSON.parseFull(jsonStr)
  val jsonMap = json.get.asInstanceOf[Map[String,String]]
  val message = jsonMap.get("message")
  message.getOrElse("").replace("\r","")
}
  def avgUser(its:Iterable[String]):String={
    val count:Long = its.size
    val list:Iterable[Long] = its.map(format.parse(_).getTime)
    val max:Long = list.max
    val min:Long = list.min
    println(s"${its.toBuffer} max=$max,min=$min count=$count")
    ((max-min)/count).toString
  }


}
