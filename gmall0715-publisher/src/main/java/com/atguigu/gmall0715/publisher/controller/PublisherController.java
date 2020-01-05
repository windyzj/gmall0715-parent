package com.atguigu.gmall0715.publisher.controller;


import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0715.publisher.bean.Option;
import com.atguigu.gmall0715.publisher.bean.Stat;
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



    @GetMapping("sale_detail")
    public String  getSaleDetail(@RequestParam("date") String date ,@RequestParam("startpage") int pageNo,
                                 @RequestParam("size") int size, @RequestParam("keyword") String keyword ){

        Map saleDetailMap = publisherService.getSaleDetail(date, keyword, pageNo, size);
        Long total =(Long)saleDetailMap.get("total");
        List detail =(List) saleDetailMap.get("detail");

        Map ageMap = (Map)saleDetailMap.get("ageAgg");


        Long age_20ct=0L;
        Long age20_30ct=0L;
        Long age30_ct=0L;

        for (Object o : ageMap.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            String ageKey =(String) entry.getKey();
            Long ageCount =(Long)entry.getValue();
            Integer age = Integer.valueOf(ageKey);

            if(age<20){
                age_20ct+=ageCount;
            }else if(age>=20 &&age<30){
                age20_30ct+=ageCount;
            }else {
                age30_ct+=ageCount;
            }
        }

        Double age_20ratio = Math.round(age_20ct * 1000D / total) / 10D;
        Double age20_30ratio = Math.round(age20_30ct * 1000D / total) / 10D;
        Double age30_ratio = Math.round(age30_ct * 1000D / total) / 10D;


        List<Stat>  statList=new ArrayList<>();

        List<Option>  ageOptionList=new ArrayList<>();
        ageOptionList.add(new Option("20岁以下",age_20ratio));
        ageOptionList.add(new Option("20岁到30岁",age20_30ratio));
        ageOptionList.add(new Option("30岁及以上",age30_ratio));
        statList.add(new Stat("用户年龄占比",ageOptionList)) ;

        Map genderMap = (Map)saleDetailMap.get("genderAgg");

        Long femaleCount = (Long)genderMap.get("F");
        Long  maleCount = (Long)genderMap.get("M");
        Double femaleRatio = Math.round(femaleCount * 1000D / total) / 10D;
        Double maleRatio = Math.round(maleCount * 1000D / total) / 10D;

        List<Option>  genderOptionList=new ArrayList<>();
        genderOptionList.add(new Option("男",maleRatio));
        genderOptionList.add(new Option("女",femaleRatio));
        statList.add(new Stat("用户性别占比",genderOptionList)) ;


        Map result =new HashMap();
        result.put("stat",statList);
        result.put("detail",detail);
        result.put("total",total);

        return JSON.toJSONString(result)     ;

    }


}
