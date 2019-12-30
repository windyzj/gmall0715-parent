package com.atguigu.gmall0715.publisher.service.impl;

import com.atguigu.gmall0715.publisher.mapper.DauMapper;
import com.atguigu.gmall0715.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Override
    public Long getDauCount(String date) {
        return dauMapper.selectDauCount(date);
    }

    @Override
    public Map getDauCountHour(String date) {

        Map resultMap=new HashMap();

        //结构转换
        //  [{"hour":15,"ct":428},{"hour":16,"ct":667}]
        List<Map> mapList = dauMapper.selectDauCountHour(date);
        //{"15":428,"16":667}
        for (Map map : mapList) {
            resultMap.put(  map.get("hour"),map.get("ct"));
        }

        return resultMap;
    }


}
