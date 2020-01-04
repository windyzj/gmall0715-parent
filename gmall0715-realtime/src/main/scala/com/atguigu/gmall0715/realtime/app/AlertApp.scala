package com.atguigu.gmall0715.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0715.common.constant.GmallConstant
import com.atguigu.gmall0715.realtime.bean.{CouponAlertInfo, EventInfo}
import com.atguigu.gmall0715.realtime.util.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks

object AlertApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("alert_app")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_EVENT,ssc)


    val eventInfoDstream: DStream[EventInfo] = inputDstream.map { record =>
      val jsonString: String = record.value()
      val eventInfo: EventInfo = JSON.parseObject(jsonString, classOf[EventInfo])
      val datetime = new Date(eventInfo.ts)
      val formattor = new SimpleDateFormat("yyyy-MM-dd HH")
      val datetimeStr: String = formattor.format(datetime)
      val datetimeArr: Array[String] = datetimeStr.split(" ")
      eventInfo.logDate = datetimeArr(0)
      eventInfo.logHour = datetimeArr(1)
      eventInfo

    }

//    1 同一设备  groupbykey (mid)
//    2 5分钟内  开窗口  window (窗口大小 （5分钟）， 滑动步长(10秒) )
//    3 三次及以上用不同账号登录并领取优惠劵    业务判断  判断同一个设备的操作行为组 是否满足条件
//    4 并且在登录到领劵过程中没有浏览商品
//    5 整理成要保存预警的格式

    //5分钟内  开窗口  window (窗口大小 （5分钟）， 滑动步长(10秒) )
    eventInfoDstream.cache()
    val eventWindowDstream: DStream[EventInfo] = eventInfoDstream.window(Seconds(300),Seconds(5))
    eventWindowDstream.cache()

    // 分组 按照mid
    val eventInfoGroupMidDStream: DStream[(String, Iterable[EventInfo])] = eventWindowDstream.map(eventInfo=>(eventInfo.mid,eventInfo)).groupByKey()
    eventInfoGroupMidDStream.cache()
    val alertInfoDstream: DStream[(Boolean, CouponAlertInfo)] = eventInfoGroupMidDStream.map { case (mid, eventInfoItr) =>
      //业务判断
      //找出所有领取优惠券是登录的uid
      val uidSet = new util.HashSet[String]()
      val itemSet = new util.HashSet[String]()
      val eventList = new util.ArrayList[String]()
      var ifAlert = false
      var isClickItem = false

      Breaks.breakable(
        for (eventInfo: EventInfo <- eventInfoItr) {
          eventList.add(eventInfo.evid)
          if (eventInfo.evid == "coupon") { //保存领取中的登录账号
            uidSet.add(eventInfo.uid)
            itemSet.add(eventInfo.itemid)
          }
          if (eventInfo.evid == "clickItem") {  //判断是否有点击商品
            isClickItem = true
            Breaks.break
          }
        }
      )

      if (uidSet.size() >= 3 && isClickItem == false) {  //三个账号且没点击商品 符合预警条件
        ifAlert = true
      }
      (ifAlert, CouponAlertInfo(mid, uidSet, itemSet, eventList, System.currentTimeMillis()))
    }

    val filteredDstream: DStream[(Boolean, CouponAlertInfo)] = alertInfoDstream.filter(_._1)




  //  filteredDstream.print(100)


    filteredDstream.foreachRDD{rdd=>

      rdd.foreachPartition{ alertItr=>
       // 6 同一设备，每分钟只记录一次预警 去重
        // 利用要保存到的数据库的 幂等性进行去重  PUT
        // ES的幂等性 是基于ID    设备+分钟级时间戳作为id


        val sourceList: List[(String, CouponAlertInfo)] = alertItr.map { case (flag, alertInfo) =>
          (alertInfo.mid + "_" + alertInfo.ts / 1000 / 60, alertInfo)
        }.toList
        MyEsUtil.insertEsBulk(GmallConstant.ES_INDEX_ALERT,sourceList)

      }

    }

    ssc.start()
    ssc.awaitTermination()
  }

}
