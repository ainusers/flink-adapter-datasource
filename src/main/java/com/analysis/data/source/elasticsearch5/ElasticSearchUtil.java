package com.analysis.data.sink.elasticsearch5;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 项目名称: sirius-sdk
 * 文件名称: com.qax.situation.sirius.sdk.infra.persistence.datasource.elasticsearch
 * 描述: [ElasticSearch工具类]
 * 创建时间: 2021/10/27.
 * 公司信息: xxx Group.Situation xxx事业部
 *
 * @author tianyong@xxx
 * @version v2.0
 */
@Slf4j
public class ElasticSearchUtil implements Serializable {

    private static final String SCROLL_ID = "scrollId";

    /**
      * @time: 2021/10/27 17:49
      * @Param: 输入查询条件对象
      * @return: 返回查询结果
      * @Description: 根据条件查询数据
      */
    public static List<String> searchDataPage(TransportClient client, SearchRequest searchRequest) {
        SearchResponse searchResponse = client.search(searchRequest).actionGet();
        if (200 == searchResponse.status().getStatus()) {
            return Arrays.stream(searchResponse.getHits().getHits())
                    .map(n -> {
                        Map<String, Object> source = n.getSource();
                        source.put("alert_src",2);
                        return JSON.toJSONString(source);
                    })
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    /**
     * @time: 2021/11/22 11:03
     * @Param:
     * @return:
     * @Description: 滚动查询
     */
    public static JSONObject scrollSearchData(TransportClient client, SearchRequest searchRequest, JSONObject dataSet) {
        if (dataSet.getString(SCROLL_ID) == null) {
            return firstScrollSearch(client, searchRequest,dataSet);
        } else {
            return secondScrollSearch(client, dataSet);
        }
    }

    /**
     * @time: 2021/11/22 10:37
     * @Param:
     * @return:
     * @Description: 第一次滚动查询
     */
    public static JSONObject firstScrollSearch(TransportClient client, SearchRequest searchRequest,JSONObject dataSet) {
        SearchResponse searchResponse = client.search(searchRequest).actionGet();
        return getScrollSearchData(searchResponse,dataSet);
    }

    /**
     * @time: 2021/11/22 10:51
     * @Param:
     * @return:
     * @Description: 第二次滚动查询
     */
    public static JSONObject secondScrollSearch(TransportClient client, JSONObject dataSet) {
        SearchScrollRequest searchScrollRequest = new SearchScrollRequest(dataSet.getString(SCROLL_ID))
                .scroll(new Scroll(new TimeValue(60, TimeUnit.MINUTES)));
        SearchResponse searchResponse = client.searchScroll(searchScrollRequest).actionGet();
        return getScrollSearchData(searchResponse,dataSet);
    }

    /**
     * @time: 2021/11/17 17:22
     * @Param:
     * @return:
     * @Description: 清除滚动id
     */
    public static void clearscrollId(TransportClient client, String scrollId) {
        //清除滚动id
        try {
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            client.clearScroll(clearScrollRequest).actionGet();
        } catch (Exception e) {
            log.error("执行es清除滚动id时发生故障", e);
        }
    }

    /**
     * @time: 2021/11/18 10:55
     * @Param: searchResponse查询对象
     * @return: scrollId
     * @Description: 获取返回数据
     */
    public static JSONObject getScrollSearchData(SearchResponse searchResponse,JSONObject dataSet) {
        if (searchResponse != null && 200 == searchResponse.status().getStatus()) {
            List<String> scrollSearchData = Arrays.stream(searchResponse.getHits().getHits())
                    .map(n -> {
                        Map<String, Object> source = n.getSource();
                        source.put("alert_src", 2);
                        return JSON.toJSONString(source);
                    })
                    .collect(Collectors.toList());

            dataSet.put("total",searchResponse.getHits().getTotalHits());
            dataSet.put(SCROLL_ID,searchResponse.getScrollId());
            dataSet.put("searchData",scrollSearchData);
        }
        return dataSet;
    }

    /**
      * @time: 2021/10/28 14:42
      * @Param: 索引前缀 (如果参数为空字符串则查询所有索引,如果参数有值则通过前缀查询)
      * @return: 查询索引结果
      * @Description: 查询索引 (可查询全部/可查询指定前缀)
      */
    public static String[] searchIndex(TransportClient client, String indexPrefix) {
        GetIndexRequest getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(indexPrefix + "*").types();

        GetIndexResponse getIndexResponse = client.admin().indices().getIndex(getIndexRequest).actionGet();
        return getIndexResponse.getIndices();
    }
    
    /**
      * @time: 2021/12/19 15:17
      * @Param: client (client连接)、index (索引)、type (type)、alertId (索引数据字段)、data (需要更新的数据，键值对形式)
      * @return: 返回200表示成功
      * @Description: 修改es指定索引、指定id下的索引数据
      */
    public static int updateDataByIndexAndId(TransportClient client,String index,String type,String alertId, Map<String,Object> data) {
        // 不知道rowid时
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("alert_id", alertId));
        // 构建searchRequest对象
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(searchSourceBuilder);
        // 构建searchResponse对象
        SearchResponse searchResponse = client.search(searchRequest).actionGet();
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        int status = 0;
        // 当未命中更新数据，则不进行下面更新操作
        if (searchHits.length == 0) {
            return status;
        }
        for (SearchHit s:searchHits) {
            // 获取rowid
            String rowid = s.getId();
            UpdateRequest updateRequest = new UpdateRequest(s.getIndex(),type,rowid)
                    .doc(data);
            updateRequest.docAsUpsert(true);
            UpdateResponse updateResponse = client.update(updateRequest).actionGet();
            status = updateResponse.status().getStatus();
        }
        return status;
    }

    /**
      * @time: 2021/10/28 14:52
      * @Param: 索引前缀 (如果参数为空字符串则查询所有索引,如果参数有值则通过前缀查询)
      * @return: 查询mapping结果集
      * @Description: 根据索引查询对应的Mapping
      */
    public static String searchMappingByIndex(TransportClient client, String indexPrefix) {
        GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices(indexPrefix + "*").types();

        GetMappingsResponse response = client.admin().indices().getMappings(getMappingsRequest).actionGet();
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappingsByIndex = response.getMappings();
        Map<String, Map<String, Object>> result = new HashMap<>();
        for (ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> indexEntry : mappingsByIndex) {
            if (indexEntry.value.isEmpty()) {
                continue;
            }
            for (ObjectObjectCursor<String, MappingMetaData> typeEntry : indexEntry.value) {
                try {
                    if (indexEntry.key.contains("-") || indexEntry.key.contains("_")) {
                        result.put(handleString(indexEntry.key,"-","_"),typeEntry.value.sourceAsMap());
                    } else {
                        result.put(indexEntry.key,typeEntry.value.sourceAsMap());
                    }
                } catch (IOException e) {
                    log.error("执行根据es索引查询对应Mapping时发生故障",e);
                }
            }
        }
        return JSON.toJSONString(result);
    }

    /**
      * @time: 2021/10/28 15:47
      * @Param: 需要解析的字符串/主要标识符号/次要标识符号
      * @return: 解析后的字符串
      * @Description: 字符串处理：截取某字符串最后一个符号之前的子串
      */
    public static String handleString(String str,String firstSymbol,String secondSymbol) {
        String result;
        if (str.lastIndexOf(firstSymbol) != -1) {
            result = str.substring(0, str.lastIndexOf(firstSymbol));
        } else {
            result = str.substring(0,str.lastIndexOf(secondSymbol));
        }
        return result;
    }
    
    /**
      * @time: 2021/11/24 11:22
      * @Param: the localDateTime to parse such as "2021-11-24T10:15:30"
      * @return: 
      * @Description: 格式化（字符串转localDateTime转timestamp）
      */
    public static String formatLocalDateTime(String localDateTime) {
        if (13 == localDateTime.length()) {
            return localDateTime;
        }
        LocalDateTime startTime = LocalDateTime.parse(localDateTime);
        return String.valueOf(startTime.toInstant(ZoneOffset.of("+8")).toEpochMilli());
    }
}
