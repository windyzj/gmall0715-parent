package com.atguigu.gmall0715.publisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {


    public Long   selectDauCount(String date);

    public List<Map> selectDauCountHour(String date);
}
