package com.atguigu.gmall0715.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalApp {


    public static void main(String[] args) {
        //1 连接服务器端
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop1", 11111), "example", "", "");
        //2  利用连接器 抓取数据
        while(true) {
            canalConnector.connect();
            canalConnector.subscribe("gmall0715.*");
            Message message = canalConnector.get(100);// 抓100条sql 执行的数据结果    每条sql执行的结果 放在一个entry  一个message 包含 多个 entry
            List<CanalEntry.Entry> entries = message.getEntries();
            if(entries.size()==0){
                System.out.println("没有数据，休息一会");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else{
                for (CanalEntry.Entry entry : entries) {
                    // 取出序列化后的值集合
                    if(entry.getEntryType()!= CanalEntry.EntryType.ROWDATA){
                        continue;
                    }

                    ByteString storeValue = entry.getStoreValue();
                    CanalEntry.RowChange rowChange=null;
                    try {  //反序列化处理
                         rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }
                    if(rowChange!=null){
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList(); //行集
                        String tableName = entry.getHeader().getTableName(); //表名
                        CanalEntry.EventType eventType = rowChange.getEventType();//时间类型 insert ? update ? delete
                        CanalHandler canalHandler = new CanalHandler(eventType, tableName, rowDatasList);
                        canalHandler.handle();
                    }



                }



            }


        }
    }


}
