package com.analysis.data.sink.elasticsearch7;

import com.example.demo.DemoApplication;
import com.example.demo.es7.ElasticSearchService;
import com.example.demo.es7.ElasticSearchSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.springframework.boot.SpringApplication;
import java.util.List;
import java.util.Map;

/**
 * @author: tianyong
 * @time: 2021/10/12 14:45
 * @description:
 * @Version: v1.0
 * @company: xxx Group.Situation xxx事业部
 */
public class CustomElasticSearch7Source {
    public static void main(String[] args) throws Exception {
        SpringApplication.run(DemoApplication.class, args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        ElasticSearchService searchService = ElasticSearchSource.builder()
                .id("custom-id")
                .index(List.of("person").toArray(new String[1]))
                .type("custom-type")
                .field(new String[]{"id", "name", "age", "country", "addr", "data", "birthday"})
                .startPage(1)
                .pageSize(10)
                .rangeFilter(Map.of("field","id","from","2021-10-12 10:46:52","to","2021-10-12 10:46:57"))
                .build();
        List<HttpHost> hosts = List.of(new HttpHost("192.168.43.43", 9200));
        DataStreamSource dataStreamSource = env.addSource(new ElasticSearchSource(hosts,searchService));

        dataStreamSource.print("data");

        SingleOutputStreamOperator<String> dataStream = dataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String data) throws Exception {
                data = data.replace("1990-01-01", "1992-02-02");
                return data;
            }
        });

        dataStream.print("result");

        env.execute("es sink");
    }
}

