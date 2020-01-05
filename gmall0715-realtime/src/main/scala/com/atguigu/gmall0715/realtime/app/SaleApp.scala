package com.atguigu.gmall0715.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall0715.common.constant.GmallConstant
import com.atguigu.gmall0715.realtime.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.gmall0715.realtime.util.{MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object SaleApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sale_app")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val inputOrderDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER,ssc)
    val inputOrderDetailDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER_DETAIL,ssc)



    // 订单的结构转换
    val orderInfoDstream: DStream[OrderInfo] = inputOrderDstream.map { record =>
      val jsonString: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])

      val telTuple: (String, String) = orderInfo.consignee_tel.splitAt(3)

      val datetimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date=datetimeArr(0)
      orderInfo.create_hour= datetimeArr(1).split(":")(0)

      orderInfo.consignee_tel = telTuple._1 + "********"

      orderInfo
    }
    // 订单明细的结构转换
    val orderDetailDstream: DStream[OrderDetail] = inputOrderDetailDstream.map { record =>
      val jsonString: String = record.value()
      JSON.parseObject(jsonString, classOf[OrderDetail])
    }

    val orderWithIdDstream: DStream[(String, OrderInfo)] = orderInfoDstream.map(orderInfo=>(orderInfo.id,orderInfo))
    val orderDetailWithIdDstream: DStream[(String, OrderDetail)] = orderDetailDstream.map(orderDetail=>(orderDetail.order_id,orderDetail))

   // val orderJoinedDstream: DStream[(String, (OrderInfo, OrderDetail))] = orderWithIdDstream.join(orderDetailWithIdDstream)

    val orderFullJoinedDstream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderWithIdDstream.fullOuterJoin(orderDetailWithIdDstream)

    val saleDetailDstream: DStream[SaleDetail] = orderFullJoinedDstream.flatMap { case (orderId, (orderInfoOption, orderDetailOption)) =>
      val saleDetailList = new ListBuffer[SaleDetail]
      val jedis: Jedis = RedisUtil.getJedisClient

      // 1 先判断主表是否在

      if (orderInfoOption != None) {
        val orderInfo: OrderInfo = orderInfoOption.get
        //1.1 判断从表是否存在
        if (orderDetailOption != None) {
          val orderDetail: OrderDetail = orderDetailOption.get
          val saleDetail = new SaleDetail(orderInfo, orderDetail)
          saleDetailList += saleDetail
        }
        //1.2  把主表写入缓存
        //  1 type  string   2 key  order_info:[orderId]    3 value  orderInfoJson  4 expire
        val orderInfoKey = "order_info:" + orderInfo.id
        val orderInfoJson: String = JSON.toJSONString(orderInfo, new SerializeConfig(true))
        jedis.setex(orderInfoKey, 10 * 60, orderInfoJson)
        // 1.3 主表从缓存中查询从表
        val orderDetailKey = "order_detail:" + orderInfo.id
        val orderDetailJsonSet: util.Set[String] = jedis.smembers(orderDetailKey)
        import scala.collection.JavaConversions._
        for (orderDetailJson: String <- orderDetailJsonSet) {
          val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
          val saleDetail = new SaleDetail(orderInfo, orderDetail)
          saleDetailList += saleDetail
        }


      } else {
        // 2.1 从表查询缓存 获得主表信息
        val orderDetail: OrderDetail = orderDetailOption.get
        val orderInfoKey = "order_info:" + orderDetail.order_id
        val orderInfoJson: String = jedis.get(orderInfoKey)
        if (orderInfoJson != null && orderInfoJson.length > 0) {
          val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
          val saleDetail = new SaleDetail(orderInfo, orderDetail)
          saleDetailList += saleDetail
        }
        //  2.2 从表保存到redis中
        // 1 type   set          key    order_detail:[order_id]      value  order_detail json .....
        val orderDetailKey = "order_detail:" + orderDetail.order_id
        val orderDetailJson: String = JSON.toJSONString(orderDetail, new SerializeConfig(true))
        jedis.sadd(orderDetailKey, orderDetailJson)
        jedis.expire(orderDetailKey, 10 * 60)
      }
      jedis.close()
      saleDetailList

    }


    //补充用户信息
    val saleDetaiWithUserDstream: DStream[SaleDetail] = saleDetailDstream.mapPartitions { saleDetailItr =>
      val jedis: Jedis = RedisUtil.getJedisClient
      val saleDetailWithUserList = new ListBuffer[SaleDetail]
      for (saleDetail <- saleDetailItr) {
        val userKey = "user_info:" +saleDetail.user_id
        val userJson: String = jedis.get(userKey)
        val userInfo: UserInfo = JSON.parseObject(userJson, classOf[UserInfo])
        saleDetail.mergeUserInfo(userInfo)
        saleDetailWithUserList += saleDetail
      }


      jedis.close()
      saleDetailWithUserList.toIterator
    }


  //  saleDetaiWithUserDstream.print(100)

    //保存商品销售信息明细
    saleDetaiWithUserDstream.foreachRDD{rdd=>
      rdd.foreachPartition{saleDetailItr=>
        val saleDetailList: List[(String, SaleDetail)] = saleDetailItr.map(saledetail=>(saledetail.order_detail_id,saledetail)).toList
        MyEsUtil.insertEsBulk(GmallConstant.ES_INDEX_SALE,saleDetailList)

      }
    }



    val inputUserDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_USER,ssc)

//    inputUserDstream.map(_.value()).print(100)
//
    inputUserDstream.foreachRDD{rdd=>

        rdd.foreachPartition{ userItr=>
          val jedis: Jedis = RedisUtil.getJedisClient
          for (userRecord <- userItr ) {

            val userJson: String = userRecord.value()
            println("!!!!!!!!!!!"+userJson)
            val userInfo: UserInfo = JSON.parseObject(userJson,classOf[UserInfo])
            //type :  string      key :  user_info:[user_id]      value userJson
            val userKey="user_info:"+userInfo.id
            jedis.set(userKey,userJson)
          }
          jedis.close()
        }

    }



   // saleDetailDstream.print(100)


    ssc.start()
    ssc.awaitTermination()


  }

}
