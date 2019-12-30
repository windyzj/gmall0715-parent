package com.atguigu.gmall0715.publisher.service;

import java.util.Map;

public interface PublisherService {

    public   Long getDauCount(String date);

    public Map getDauCountHour(String date);
}
