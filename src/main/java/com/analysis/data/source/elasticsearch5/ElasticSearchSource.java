package com.analysis.data.sink.elasticsearch5;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.qax.situation.sirius.sdk.infra.common.LoadSettings;
import com.qax.situation.sirius.sdk.infra.entity.Source;
import com.qax.situation.sirius.sdk.infra.persistence.datasource.SourceFunction;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 项目名称: sirius-sdk
 * 文件名称: com.qax.situation.sirius.sdk.infra.persistence.datasource.elasticsearch
 * 描述: [elasticsearch-source操作类]
 * 创建时间: 2021/10/27.
 * 公司信息: xxx Group.Situation xxx事业部
 *
 * @author tianyong@xxx
 * @version v2.0
 */
@Slf4j
public class ElasticSearchSource<T> implements SourceFunction<com.qax.situation.sirius.sdk.infra.persistence.datasource.elasticsearch.ElasticSearchSource<T>> {

    private String index;
    private String field;
    private String beginTime;
    private String endTime;
    private Integer pageSize;
    private JSONObject dataSet;
    private transient TransportClient client;
    private transient PreBuiltTransportClient preBuiltTransportClient;

    /**
      * @time: 2022/1/6 20:06
      * @Param: 
      * @return: 
      * @Description: 优化代码: 去除手动调用
      */
    {
        open();
    }

    /**
      * @time: 2021/10/27 17:46
      * @Param:
      * @return: 
      * @Description: 初始化elasticsearch-client连接，赋值相关属性
      */
    @Override
    public void open() {
        Source esSource = LoadSettings.getOfflinedata().get("elasticsearch");
        Settings settings = Settings.builder().put("cluster.name", JSON.parseObject(esSource.getSourceDetail()).get("clusterName")).build();
        try {
            List<InetSocketTransportAddress> hosts = List.of(new InetSocketTransportAddress(InetAddress.getByName(
                    esSource.getSourceHost()),
                    esSource.getSourcePort()));
            InetSocketTransportAddress[] inetSocketTransportAddresses = hosts.toArray(new InetSocketTransportAddress[hosts.size()]);
            preBuiltTransportClient = new PreBuiltTransportClient(settings);
            client = preBuiltTransportClient.addTransportAddresses(inetSocketTransportAddresses);
        } catch (UnknownHostException e) {
            log.error("执行初始化elasticsearch-client连接时故障",e);
        }
    }

    /**
      * @time: 2021/10/27 17:46
      * @Param:
      * @return: 返回elasticsearch查询结果
      * @Description: 运行elasticsearch查询操作
      */
    @Override
    public JSONObject run() {
        return com.qax.situation.sirius.sdk.infra.persistence.datasource.elasticsearch.ElasticSearchUtil.scrollSearchData(client,makeSearchRequest(),dataSet);
    }

    /**
     * @time: 2021/10/27 17:45
     * @Param:
     * @return:
     * @Description: 构建makeSearchRequest对象
     */
    public SearchRequest makeSearchRequest() {
        // 构建查询对象
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 超时时间
        sourceBuilder.timeout(new TimeValue(600, TimeUnit.SECONDS));
        // 按照查询字段升序
        sourceBuilder.sort(String.valueOf(field), SortOrder.ASC);
        // 查询条件
        sourceBuilder
                .query(QueryBuilders.rangeQuery(String.valueOf(field))
                .from(beginTime)
                .to(endTime));
        // 设置分页
        sourceBuilder.size(pageSize);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(index);
        searchRequest.source(sourceBuilder);
        searchRequest.scroll(new Scroll(new TimeValue(60, TimeUnit.MINUTES)));

        return searchRequest;
    }

    /**
     * @time: 2021/10/28 14:42
     * @Param: 索引前缀 (如果参数为空字符串则查询所有索引,如果参数有值则通过前缀查询)
     * @return: 查询索引结果
     * @Description: 查询索引 (可查询全部/可查询指定前缀)
     */
    public String[] searchIndex(String indexPrefix) {
        return com.qax.situation.sirius.sdk.infra.persistence.datasource.elasticsearch.ElasticSearchUtil.searchIndex(client, indexPrefix);
    }

    /**
     * @time: 2021/10/28 14:52
     * @Param: 索引前缀 (如果参数为空字符串则查询所有索引,如果参数有值则通过前缀查询)
     * @return: 查询mapping结果集
     * @Description: 根据索引查询对应的Mapping
     */
    public String searchMappingByIndex(String indexPrefix) {
        return com.qax.situation.sirius.sdk.infra.persistence.datasource.elasticsearch.ElasticSearchUtil.searchMappingByIndex(client,indexPrefix);
    }
    
    /**
      * @time: 2021/12/19 15:17
      * @Param: client (client连接)、index (索引)、type (type)、alertId (索引数据字段)、data (需要更新的数据，键值对形式)
      * @return: 返回200表示成功
      * @Description: 修改es指定索引、指定id下的索引数据
      */
    public int updateDataByIndexAndId(String index, String type, String alertId, Map<String,Object> data) {
        return com.qax.situation.sirius.sdk.infra.persistence.datasource.elasticsearch.ElasticSearchUtil.updateDataByIndexAndId(client,index + "*",type,alertId,data);
    }

    /**
     * @time: 2021/10/27 14:48
     * @Param:
     * @return:
     * @Description: 发送参数信息
     */
    @Override
    public com.qax.situation.sirius.sdk.infra.persistence.datasource.elasticsearch.ElasticSearchSource<T> send(JSONObject jsonObject) {
        this.index = jsonObject.getString("indexName") + "*";
        this.field = jsonObject.getString("indexField");
        this.beginTime = com.qax.situation.sirius.sdk.infra.persistence.datasource.elasticsearch.ElasticSearchUtil.formatLocalDateTime(jsonObject.getString("startTime"));
        this.endTime = com.qax.situation.sirius.sdk.infra.persistence.datasource.elasticsearch.ElasticSearchUtil.formatLocalDateTime(jsonObject.getString("endTime"));
        this.pageSize = jsonObject.getInteger("batchNum");
        this.dataSet = jsonObject.getJSONObject("dataSet");
        return this;
    }

    /**
      * @time: 2021/10/27 17:48
      * @Param:
      * @return: 
      * @Description: 关闭client链接
      */
    @Override
    public void close() {
        if (dataSet != null && !dataSet.getString("scrollId").isEmpty()) {
            com.qax.situation.sirius.sdk.infra.persistence.datasource.elasticsearch.ElasticSearchUtil.clearscrollId(client,dataSet.getString("scrollId"));
        }
        preBuiltTransportClient.close();
        client.close();
    }
}
