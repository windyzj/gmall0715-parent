package com.atguigu.gmall0715.publisher.controller;


import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0715.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

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


        Map orderMap=new HashMap();
        Double orderAmount = publisherService.getOrderAmount(date);
        orderMap.put("id","order_amount");
        orderMap.put("name","新增交易额");
        orderMap.put("value",orderAmount);
        totalList.add(orderMap);

        return JSON.toJSONString(totalList);
    }

    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id ,@RequestParam("date") String date){
       if("dau".equals(id)) {
           Map dauCountHourMapTD = publisherService.getDauCountHour(date);
           String yd = getYd(date);
           Map dauCountHourMapYD = publisherService.getDauCountHour(yd);

           Map<String,Map> resultMap=new HashMap<>();
           resultMap.put("yesterday",dauCountHourMapYD);
           resultMap.put("today",dauCountHourMapTD);
           return   JSON.toJSONString(resultMap) ;
       }else if("order_amount".equals(id)){
           Map orderAmountHourMapTD = publisherService.getOrderAmountHour(date);
           String yd = getYd(date);
           Map orderAmountHourMapYD = publisherService.getOrderAmountHour(yd);

           Map<String,Map> resultMap=new HashMap<>();
           resultMap.put("yesterday",orderAmountHourMapYD);
           resultMap.put("today",orderAmountHourMapTD);
           return   JSON.toJSONString(resultMap) ;

       }


       else {
           return null;
       }

    }


    private String  getYd(String td){
        String yd=null;
        SimpleDateFormat formator = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date tdDate = formator.parse(td);
            Date ydDate = DateUtils.addDays(tdDate, -1);
            yd=formator.format(ydDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return yd;
    }

}
