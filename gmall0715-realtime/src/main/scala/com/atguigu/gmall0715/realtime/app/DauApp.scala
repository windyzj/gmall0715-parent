package com.atguigu.gmall0715.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0715.common.constant.GmallConstant
import com.atguigu.gmall0715.realtime.bean.StartupLog
import com.atguigu.gmall0715.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauApp {



//  1  消费kafka 的数据
//  2  json字符串 -> 转换为一个对象 case class
//  3  利用redis进行过滤
//  4  把过滤后的新数据进行写入 redis  ( 当日用户访问的清单)
//  5  再把数据写入到hbase中
  def main(args: Array[String]): Unit = {

      val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dau_app")

      val ssc = new StreamingContext(sparkConf,Seconds(5))
  //  1  消费kafka 的数据
      val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP,ssc)

 //     inputDstream.map(_.value()).print()

  //  2  json字符串 -> 转换为一个对象 case class
  val startUpDstream: DStream[StartupLog] = inputDstream.map { record =>

    val jsonString: String = record.value()
    val startupLog: StartupLog = JSON.parseObject(jsonString, classOf[StartupLog])

    val datetime = new Date(startupLog.ts)
    val formattor = new SimpleDateFormat("yyyy-MM-dd HH")
    val datetimeStr: String = formattor.format(datetime)
    val datetimeArr: Array[String] = datetimeStr.split(" ")
    startupLog.logDate = datetimeArr(0)
    startupLog.logHour = datetimeArr(1)

    startupLog
  }

  //3  利用广播变量 把清单发给各个executor  各个executor  根据清单进行比对 进行过滤

  val filteredDstream: DStream[StartupLog] = startUpDstream.transform { rdd =>
    //driver  周期性执行
    val jedis: Jedis = RedisUtil.getJedisClient
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val today: String = format.format(new Date())
    val dauKey: String = "dau:" + today
    val dauSet: util.Set[String] = jedis.smembers(dauKey)
    jedis.close()
    val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
    println("过滤前:"+rdd.count()+"条")
    val filteredRDD: RDD[StartupLog] = rdd.filter { startup => //executor

      val dauSet: util.Set[String] = dauBC.value
      !dauSet.contains(startup.mid)
    }
    println("过滤后:"+filteredRDD.count()+"条")
    filteredRDD
  }


  //本批次 自检去重
  // 相同的mid 保留第一条
  val groupbyMidDStream: DStream[(String, Iterable[StartupLog])] = filteredDstream.map(startuplog=>(startuplog.mid,startuplog)).groupByKey()
  val filteredSefDstream: DStream[StartupLog] = groupbyMidDStream.map { case (mid, startupLogItr) =>
    val top1list: List[StartupLog] = startupLogItr.toList.sortWith((startupLog1, startupLog2) => startupLog1.ts < startupLog2.ts).take(1)
    top1list(0)
  }









  //  3  利用redis进行过滤  //反复连接redis  可以优化
//  startUpDstream.filter{ startup=>
//    val jedis: Jedis = RedisUtil.getJedisClient
//    val dauKey: String = "dau:"+ startup.logDate
//    val exists: Boolean = jedis.sismember( dauKey, startup.mid)
//    !exists
//  }





//  4  把过滤后的新数据进行写入 redis  ( 当日用户访问的清单)
  filteredSefDstream.foreachRDD{rdd=>

    rdd.foreachPartition{startupItr=>
      val jedis: Jedis = RedisUtil.getJedisClient

      for (startup <- startupItr ) {
        //  保存当日用户的访问清单 string list set hash zset
        // jedis   type :  set    key :   dau:[日期]   value: mid
           val dauKey: String = "dau:"+ startup.logDate
          println(startup)
          jedis.sadd(dauKey,startup.mid)
      }
      jedis.close()


    }

  }

  //保存hbase

      ssc.start()
      ssc.awaitTermination()

  }

}
