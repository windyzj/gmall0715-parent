package com.atguigu.gmall0715.publisher.controller;


import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0715.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;


    @GetMapping("realtime-total")
    public String  getRealtimeTotal( @RequestParam("date") String date){
        Long dauCount = publisherService.getDauCount(date);

        List<Map> totalList=new ArrayList<>();

        Map dauMap=new HashMap();

        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauCount);
        totalList.add(dauMap);

        Map newMidMap=new HashMap();

        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",233);
        totalList.add(newMidMap);

        return JSON.toJSONString(totalList);
    }


}
