package com.atguigu.gmall0715.publisher.service;

import java.util.Map;

public interface PublisherService {

    public   Long getDauCount(String date);

    public Map getDauCountHour(String date);

    public   Double getOrderAmount(String date);

    public Map getOrderAmountHour (String date);
}
