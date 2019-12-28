package com.atguigu.gmall0715.publisher.service.impl;

import com.atguigu.gmall0715.publisher.mapper.DauMapper;
import com.atguigu.gmall0715.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Override
    public Long getDauCount(String date) {
        return dauMapper.selectDauCount(date);
    }
}
