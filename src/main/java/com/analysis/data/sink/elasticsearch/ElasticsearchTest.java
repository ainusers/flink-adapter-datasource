package com.analysis.data.sink.elasticsearch;

import org.springframework.boot.test.context.SpringBootTest;

//import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
//import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
//import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;

@SpringBootTest
class ElasticsearchTest {

    /*@Test
    void contextLoads() throws Exception {
        // 获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从集合中读取数据
        DataStream<String> dataStream = env.fromCollection(List.of("cc"));
        // 数据发送到elasticsearch
        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", "elasticsearch");
        config.put("bulk.flush.max.actions", "1");

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("192.168.43.46"), 9200));
        dataStream.addSink(new ElasticsearchSink(config,transportAddresses,new ElasticsearchMapper()));
        // 打印读取数据
        dataStream.print();
        // 执行任务
        env.execute();
    }*/

    /*@Test
    void contextLoads2() throws Exception {
        // 获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从集合中读取数据
        DataStream<String> dataStream = env.fromCollection(List.of("cc"));
        // 数据发送到elasticsearch
        List<HttpHost> httpHosts = List.of(new HttpHost("10.95.102.40", 9200));
        dataStream.addSink(new ElasticsearchSink.Builder<String>(httpHosts,new ElasticsearchMapper()).build());
        // 打印读取数据
        dataStream.print();
        // 执行任务
        env.execute();
    }

    public static class ElasticsearchMapper implements ElasticsearchSinkFunction<String> {
        @Override
        public void process(String s, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
            // 组装数据
            Map<String, String> datas = Map.of("id", s);
            // 组织es结构
            IndexRequest indexRequest = Requests.indexRequest()
                    .id("flag")
                    .index("senor")
                    .type("type")
                    .source(datas);
            // 发送数据
            requestIndexer.add(indexRequest);
        }
    }*/

}
