package com.analysis.data.sink.elasticsearch5;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.qax.situation.sirius.sdk.infra.common.SourceDataContext;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;
import sdk.BaseTest;

import java.util.Map;

import static org.junit.Assert.assertNotNull;

/**
 * 项目名称: sirius-sdk
 * 文件名称: sdk.infra.persistence.datasource.elasticsearch
 * 描述: [功能描述]
 * 创建时间: 2021/11/2.
 * 公司信息: xxx Group.Situation xxx事业部
 *
 * @author tianyong@xxx
 * @version v2.0
 */
@Slf4j
@RunWith(SpringRunner.class)
public class ElasticsearchTest extends BaseTest {

    @Before
    public void mockService() {}

    @Test
    public void testNotifyTaskDelayApply() throws Exception {
        SourceDataContext elasticsearch = new SourceDataContext("elasticsearch");

        JSONObject jo = new JSONObject();
        elasticsearch
                .send(JSON.parseObject(JSON.toJSONString(Map.of("indexName", "situation-abnormal_flow", "indexField", "collect_time",
                        "startTime", "1622908800000","endTime", "1622995199000", "batchNum", 100,"dataSet",jo))));
        // 根据时间范围查询数据
        jo = elasticsearch.run();
        assertNotNull(jo.get("searchData"));
        elasticsearch.close();

        ElasticSearchSource elasticSearchSource = new ElasticSearchSource();
        // 查询索引
        assertNotNull(elasticSearchSource.searchIndex("situation-sirius-2021.03.21"));
        // 根据索引查询mapping
        assertNotNull(elasticSearchSource.searchMappingByIndex("situation-sirius-2021.03.21"));
        elasticSearchSource.close();

        // 修改索引数据
        ElasticSearchSource elasticSearchSource2 = new ElasticSearchSource();
        // 当返回200标识成功
        assertNotNull(elasticSearchSource2.updateDataByIndexAndId("situation-sirius-2021.03.21", "sirius", "bc096434-7ca3-4fea-b506-322027c80509",
                Map.of("alert_src", "0")));
        elasticSearchSource2.close();
    }

    @After
    public void cleanData() {}

}

