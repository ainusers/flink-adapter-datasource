package com.analysis.data.sink.elasticsearch7;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author: tianyong
 * @time: 2021/10/15 10:16
 * @description:
 * @Version: v1.0
 * @company: xxx Group.Situation xxx事业部
 */
@Slf4j
public class ElasticSearchUtil implements Serializable {


    private static RestHighLevelClient client;


    /**
     * @author: tianyong
     * @time: 2021/10/14 17:34
     * @description: 初始化client
     * @Version: v1.0
     * @company: xxx Group.Situation xxx事业部
     */
    public static void init(RestHighLevelClient rhlClient) {
        client = rhlClient;
    }


    /**
     * @author: tianyong
     * @time: 2021/10/14 17:34
     * @description: 关闭通信client连接
     * @Version: v1.0
     * @company: xxx Group.Situation xxx事业部
     */
    public static void close() throws Exception{
        client.close();
    }


    /**
     * @author: tianyong
     * @time: 2021/10/15 9:54
     * @description: 根据条件查询数据
     * @Version: v1.0
     * @company: xxx Group.Situation xxx事业部
     */
    public static List<Map<String, Object>> SearchDataPage(SearchRequest searchRequest) {
        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            log.info("统计：本次查询共查出{}条数据", searchResponse.getHits().getTotalHits().value);
            if (200 == searchResponse.status().getStatus()) {
                return Arrays.asList(searchResponse.getHits().getHits())
                        .stream()
                        .map(n -> n.getSourceAsMap())
                        .collect(Collectors.toList());
            }
        } catch (IOException e) {
            log.error("根据条件查询数据发生错误:{}", e);
        }
        return null;
    }


    /**
     * @author: tianyong
     * @time: 2021/10/14 17:40
     * @description: 获取当前时间 (默认格式：yyyy-MM-dd HH:mm:ss)
     * @Version: v1.0
     * @company: xxx Group.Situation xxx事业部
     */
    public static String getDateTime(String pattern) {
        pattern = StringUtils.isEmpty(pattern) ? "yyyy-MM-dd HH:mm:ss" : pattern;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(pattern);
            return sdf.format(new Date());
        } catch (Exception e) {
            log.error("获取当前时间,解析时间失败：{}", e);
        }
        return null;
    }

}
