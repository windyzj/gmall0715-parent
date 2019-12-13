package com.atguigu.gmall0715.logger.controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall0715.common.constant.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@RestController  // controller+responsebody
@Slf4j
public class LoggerController {

    @Value("")
    String config123;

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @PostMapping("log")
    public String log(@RequestParam("logString") String logString){
        // 1 加时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts",System.currentTimeMillis());

        // 2 落盘成文件
        // log4j logback  log4j2  logging
        String jsonString = jsonObject.toJSONString();
        log.info(jsonString);
        //3 发送kafka
        if("startup".equals(jsonObject.getString("type")) ){
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP,jsonString);
        }else{
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT,jsonString);
        }

        return  "success";
    }
}
