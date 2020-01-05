package com.atguigu.gmall0715.publisher.service.impl;

import com.atguigu.gmall0715.common.constant.GmallConstant;
import com.atguigu.gmall0715.publisher.mapper.DauMapper;
import com.atguigu.gmall0715.publisher.mapper.OrderMapper;
import com.atguigu.gmall0715.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.apache.ibatis.mapping.ResultMap;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Autowired
    JestClient jestClient;

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

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmount(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {

        List<Map> mapList = orderMapper.selectOrderAmountHour(date);
        Map resultMap=new HashMap();
        for (Map map : mapList) {
            resultMap.put(map.get("CREATE_HOUR"),map.get("ORDER_AMOUNT"));
        }

        return resultMap;
    }

    @Override
    public Map getSaleDetail(String date, String keyword, int pageNo, int pagesize) {
        String query ="{\n" +
                "   \"query\": {\n" +
                "     \"bool\": {\n" +
                "       \"filter\": {\n" +
                "         \"term\": {\n" +
                "           \"dt\": \"2020-01-05\"\n" +
                "         }\n" +
                "         \n" +
                "       },\"must\": [\n" +
                "         {\n" +
                "           \"match\": {\n" +
                "             \"sku_name\":{\n" +
                "               \"query\": \"小米手机\",\n" +
                "               \"operator\": \"and\"\n" +
                "             }\n" +
                "           }\n" +
                "         }\n" +
                "       ]\n" +
                "     }\n" +
                " \n" +
                "   },\n" +
                "   \"aggs\": {\n" +
                "     \"groupby_user_gender\": {\n" +
                "       \"terms\": {\n" +
                "         \"field\": \"user_gender\",\n" +
                "         \"size\": 2\n" +
                "       }\n" +
                "     },\n" +
                "     \"groupby_user_age\":{\n" +
                "       \"terms\": {\n" +
                "         \"field\": \"user_age\",\n" +
                "         \"size\": 100\n" +
                "       }\n" +
                "       \n" +
                "     }\n" +
                "   }\n" +
                "   \n" +
                "   , \"from\": 0\n" +
                "   , \"size\": 20\n" +
                "   \n" +
                "   \n" +
                " }";

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //过滤 匹配
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt",date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name",keyword).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        //聚合
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_user_gender").field("user_gender").size(2);
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_user_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(genderAggs);
        searchSourceBuilder.aggregation(ageAggs);

        //分页
        searchSourceBuilder.from((pageNo-1) *pagesize );
        searchSourceBuilder.size(pagesize);

        String querySearch = searchSourceBuilder.toString();
        System.out.println(querySearch);
        Search search = new Search.Builder(querySearch).addIndex(GmallConstant.ES_INDEX_SALE).addType("_doc").build();

        Map resultMap=new HashMap();
        try {
            SearchResult searchResult = jestClient.execute(search);

             // 明细
            List<Map> saleDetailList=new ArrayList<>();
            List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
            for (SearchResult.Hit<Map, Void> hit : hits) {
                saleDetailList.add( hit.source) ;
            }

            Map ageMap=new HashMap();
            //  年龄聚合
            List<TermsAggregation.Entry> ageBuckets = searchResult.getAggregations().getTermsAggregation("groupby_user_age").getBuckets();
            for (TermsAggregation.Entry ageBucket : ageBuckets) {
                ageMap.put(ageBucket.getKey(),ageBucket.getCount());
            }

            //  性别聚合
            Map genderMap=new HashMap();
            List<TermsAggregation.Entry> genderBuckets = searchResult.getAggregations().getTermsAggregation("groupby_user_gender").getBuckets();
            for (TermsAggregation.Entry genderBucket : genderBuckets) {
                genderMap.put(genderBucket.getKey(),genderBucket.getCount());
            }

            resultMap.put("detail",saleDetailList);
            resultMap.put("ageAgg",ageMap);
            resultMap.put("genderAgg",genderMap);
            resultMap.put("total",searchResult.getTotal());




        } catch (IOException e) {
            e.printStackTrace();
        }

        return resultMap;
    }


}
