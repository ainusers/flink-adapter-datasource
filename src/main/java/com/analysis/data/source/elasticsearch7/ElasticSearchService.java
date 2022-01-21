package com.analysis.data.sink.elasticsearch7;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author: tianyong
 * @time: 2021/10/15 10:20
 * @description:
 * @Version: v1.0
 * @company: xxx Group.Situation xxx事业部
 */
@Slf4j
public class ElasticSearchService implements Serializable {

    private String id;
    private String[] index;
    private String type;
    private String[] fields;
    private Integer startPage;
    private Integer pageSize;
    private Map<String,Object> rangeFilter;


    public ElasticSearchService(String id, String[] index, String type, String[] fields, Integer startPage, Integer pageSize, Map<String,Object> rangeFilter) {
        this.id = id;
        this.index = index;
        this.type = type;
        this.fields = fields;
        this.startPage = startPage;
        this.pageSize = pageSize;
        this.rangeFilter = rangeFilter;
    }


    /**
     * @author: tianyong
     * @time: 2021/10/14 18:42
     * @description: 查询数据
     * @Version: v1.0
     * @company: xxx Group.Situation xxx事业部
     */
    public List<Map<String, Object>> search() {
        // 构建查询对象
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        // 查询字段
        sourceBuilder.fetchSource(fields, null);
        // 超时时间
        sourceBuilder.timeout(new TimeValue(30, TimeUnit.SECONDS));
        // 是否按匹配度排序
        sourceBuilder.explain(true);
        // 查询条件
        sourceBuilder.query(QueryBuilders.rangeQuery(String.valueOf(rangeFilter.get("field"))).from(rangeFilter.get("from")).to(rangeFilter.get("to")));
        // 设置分页
        sourceBuilder.from((startPage - 1) * pageSize).size(pageSize);
        log.info("统计：查询条件：{}" + sourceBuilder.toString());

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(index);
        searchRequest.source(sourceBuilder);

        return ElasticSearchUtil.SearchDataPage(searchRequest);
    }

}
