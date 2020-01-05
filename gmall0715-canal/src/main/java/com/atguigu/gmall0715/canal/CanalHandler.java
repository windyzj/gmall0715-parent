package com.atguigu.gmall0715.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall0715.common.constant.GmallConstant;

import java.util.List;

public class CanalHandler {

    CanalEntry.EventType eventType;

    String tableName;

    List<CanalEntry.RowData> rowDataList;

    public CanalHandler(CanalEntry.EventType eventType, String tableName, List<CanalEntry.RowData> rowDataList) {
        this.eventType = eventType;
        this.tableName = tableName;
        this.rowDataList = rowDataList;
    }

    public void  handle(){
        if(tableName.equals("order_info")&&eventType== CanalEntry.EventType.INSERT&&rowDataList.size()>0) {// 下单
            sendToKafka(GmallConstant.KAFKA_TOPIC_ORDER);
        }else if(tableName.equals("order_detail")&&eventType== CanalEntry.EventType.INSERT&&rowDataList.size()>0){ // 用户 商品 订单明细
            sendToKafka(GmallConstant.KAFKA_TOPIC_ORDER_DETAIL);
        }else if(tableName.equals("user_info")&&(eventType== CanalEntry.EventType.INSERT||eventType== CanalEntry.EventType.UPDATE)&&rowDataList.size()>0){ // 用户 商品 订单明细
            sendToKafka(GmallConstant.KAFKA_TOPIC_USER);
        }
    }

    public void   sendToKafka(String topic ){  //发送到kafka
        for (CanalEntry.RowData rowData : rowDataList) {
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                System.out.println(column.getName()+"---->"+ column.getValue());

                jsonObject.put(column.getName(),column.getValue());
            }

            KafkaSender.send(topic,jsonObject.toJSONString());
            try {
                Thread.sleep(1500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


}
