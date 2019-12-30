package com.atguigu.gmall0715.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0715.common.constant.GmallConstant
import com.atguigu.gmall0715.realtime.bean.OrderInfo
import com.atguigu.gmall0715.realtime.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object OrderApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("order_app")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER,ssc)

    //
    val orderInfoDstream: DStream[OrderInfo] = inputDstream.map { record =>
      val jsonString: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])

      val telTuple: (String, String) = orderInfo.consignee_tel.splitAt(3)

      val datetimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date=datetimeArr(0)
      orderInfo.create_hour= datetimeArr(1).split(":")(0)

      orderInfo.consignee_tel = telTuple._1 + "********"


      orderInfo
    }
    orderInfoDstream    // 增加一个 字段 ： 是否是该用户首次下单的标志  is_first_order


    orderInfoDstream.foreachRDD{rdd=>
      rdd.saveToPhoenix("GMALL0715_ORDER_INFO",Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),new Configuration(),Some("hadoop1,hadoop2,hadoop3:2181"))
    }


    ssc.start()
    ssc.awaitTermination()


  }

}
