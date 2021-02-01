package com.bupt.gmall2020.realtime.app

import java.util.Date
import java.text.SimpleDateFormat

import com.alibaba.fastjson.{JSON, JSONObject}
import com.bupt.gmall2020.realtime.bean.DauInfo
import com.bupt.gmall2020.realtime.util.{MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * @author yangkun
 * @date 2021/2/1 15:07
 * @version 1.0
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("app")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    val topic = "GMALL_STARTUP"
    val groupId = "DAU_GROUP"
    val recordInputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc)

//    recordInputDStream.map(_.value()).print()

    val jsonObjDstream: DStream[JSONObject] = recordInputDStream.map { record =>
      val jsonString: String = record.value()
      val jsonObj: JSONObject = JSON.parseObject(jsonString)
      val ts:java.lang.Long = jsonObj.getLong("ts")
      val datehourString: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
      val dateHour: Array[String] = datehourString.split(" ")

      jsonObj.put("dt", dateHour(0))
      jsonObj.put("hr", dateHour(1))

      jsonObj
    }

    //去重思路： 利用redis保存今天访问过系统的用户清单
    //清单在redis中保存
    //redis :  type set   string hash list set zset       key ? dau:2020-06-17        value?  mid   (field? score?)  (expire?) 24小时
    /*    val filteredDstream: DStream[JSONObject] = jsonObjDstream.filter { jsonObj =>
          val dt: String = jsonObj.getString("dt")
          val mid: String = jsonObj.getJSONObject("common").getString("mid")
          val jedis: Jedis = RedisUtil.getJedisClient
          val dauKey = "dau:" + dt
          val isNew: lang.Long = jedis.sadd(dauKey, mid) //如果未存在则保存 返回1  如果已经存在则不保存 返回0
          jedis.close()
          if (isNew == 1L) {
            true
          } else {
            false
          }
        }*/

    // println("过滤前::："+jsonObjDstream.count())
    val filteredDstream: DStream[JSONObject] = jsonObjDstream.mapPartitions { jsonObjItr =>
      val jedis: Jedis = RedisUtil.getJedisClient //一个分区只申请一次连接
      val filteredList=new ListBuffer[JSONObject]()
      //  Iterator 只能迭代一次 包括取size   所以要取size 要把迭代器转为别的容器
      val jsonList: List[JSONObject] = jsonObjItr.toList
      //   println("过滤前："+jsonList.size)
      for (jsonObj <- jsonList) {
        val dt: String = jsonObj.getString("dt")
        val mid: String = jsonObj.getJSONObject("common").getString("mid")
        val dauKey = "dau:" + dt
        val isNew: java.lang.Long = jedis.sadd(dauKey, mid) //如果未存在则保存 返回1  如果已经存在则不保存 返回0
        jedis.expire(dauKey,3600*24)
        if (isNew == 1L) {
          filteredList+=jsonObj
        }
      }
      jedis.close()
      // println("过滤后："+filteredList.size)
      filteredList.toIterator
    }
//    filteredDstream.print(10)
    filteredDstream.foreachRDD { rdd =>
      rdd.foreachPartition{jsonItr=>
       val list: List[JSONObject] = jsonItr.toList
        val dauList: List[DauInfo] = list.map { jsonObj =>
          val commonJSONObj: JSONObject = jsonObj.getJSONObject("common")
          val dauInfo = DauInfo(commonJSONObj.getString("mid"),
            commonJSONObj.getString("uid"),
            commonJSONObj.getString("ar"),
            commonJSONObj.getString("ch"),
            commonJSONObj.getString("vc"),
            jsonObj.getString("dt"),
            jsonObj.getString("hr"),
            "00",
            jsonObj.getLong("ts")
          )
          dauInfo
        }
        val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        MyEsUtil.bulkDoc(dauList,"gmall_dau_info_"+dt)

      }
    }
    ssc.start()
    ssc.awaitTermination()

  }
}
