package com.analysis.data.sink.elasticsearch7;

import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author: tianyong
 * @time: 2021/10/15 10:23
 * @description:
 * @Version: v1.0
 * @company: xxx Group.Situation xxx事业部
 */
public class ElasticSearchSource extends RichSourceFunction<String> implements Serializable {

    private List<HttpHost> httpHosts;
    private ElasticSearchService searchService;

    public ElasticSearchSource(List<HttpHost> httpHosts, ElasticSearchService searchService) {
        this.httpHosts = httpHosts;
        this.searchService = searchService;
    }

    /**
      * @author: tianyong
      * @time: 2021/10/15 10:35
      * @description: 初始化elasticsearch-client连接，赋值相关属性
      * @Version: v1.0
      * @company: xxx Group.Situation xxx事业部
      */
    @Override
    public void open(Configuration parameters) throws Exception {
        HttpHost[] httpHosts = this.httpHosts.toArray(new HttpHost[this.httpHosts.size()]);
        ElasticSearchUtil.init(new RestHighLevelClient(RestClient.builder(httpHosts)));
    }


    /**
      * @author: tianyong
      * @time: 2021/10/15 10:34
      * @description: 运行elasticsearch查询事件
      * @Version: v1.0
      * @company: xxx Group.Situation xxx事业部
      */
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        ctx.collect(searchService.search().toString());
        // this.cancel();
    }


    @SneakyThrows
    @Override
    public void cancel() {
        ElasticSearchUtil.close();
        System.exit(0);
    }


    /**
     * @author: tianyong
     * @time: 2021/10/14 19:01
     * @description: 构建elasticsearch对象属性
     * @Version: v1.0
     * @company: xxx Group.Situation xxx事业部
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements Serializable{

        private String id;
        private String[] index;
        private String type;
        private String[] fields;
        private Integer startPage;
        private Integer pageSize;
        private Map<String,Object> rangeFilter;

        public Builder() {
        }

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder index(String[] index) {
            this.index = index;
            return this;
        }

        // 注：es6.x以后的版本已不再支持type属性，默认为：_doc
        @Deprecated
        public Builder type(String type) {
            this.type = type;
            return this;
        }

        public Builder field(String[] fields) {
            this.fields = fields;
            return this;
        }

        public Builder startPage(Integer startPage) {
            this.startPage = startPage;
            return this;
        }

        public Builder pageSize(Integer pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public Builder rangeFilter(Map<String,Object> rangeFilter) {
            this.rangeFilter = rangeFilter;
            return this;
        }

        public ElasticSearchService build() {
            return new ElasticSearchService(id, index, type, fields, startPage, pageSize,rangeFilter);
        }
    }
}