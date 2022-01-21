package com.analysis.data.sink.clickhouse;

import com.example.demo.adapter.SinkContext;
import com.example.demo.ck.clickhouse.User;
import com.example.demo.ck.model.ClickHouseClusterSettings;
import com.example.demo.ck.model.ClickHouseSinkConst;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@SpringBootApplication
public class ClickhouseTest {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(DemoApplication.class, args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        Map<String, String> globalParameters = new HashMap<>();

        // ClickHouse cluster properties
        globalParameters.put(ClickHouseClusterSettings.CLICKHOUSE_HOSTS, "http://192.168.43.43:8123/");
        globalParameters.put(ClickHouseClusterSettings.CLICKHOUSE_USER, "default");
        globalParameters.put(ClickHouseClusterSettings.CLICKHOUSE_PASSWORD, "root1234");

        // sink common
        globalParameters.put(ClickHouseSinkConst.TIMEOUT_SEC, "1");
        globalParameters.put(ClickHouseSinkConst.FAILED_RECORDS_PATH, "C:\\Users\\tianyong\\Desktop");
        globalParameters.put(ClickHouseSinkConst.NUM_WRITERS, "2");
        globalParameters.put(ClickHouseSinkConst.NUM_RETRIES, "2");
        globalParameters.put(ClickHouseSinkConst.QUEUE_MAX_CAPACITY, "2");
        globalParameters.put(ClickHouseSinkConst.IGNORING_CLICKHOUSE_SENDING_EXCEPTION_ENABLED, "false");

        // set global paramaters
        ParameterTool parameters = ParameterTool.fromMap(globalParameters);
        env.getConfig().setGlobalJobParameters(parameters);

        env.setParallelism(1);

        // 从kafka中读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.43.80.80:9092");
        DataStream<String> inputStream = env.addSource(
                new FlinkKafkaConsumer<>("from-kafka", new SimpleStringSchema(), properties)
        );

        // Transform 操作
        SingleOutputStreamOperator<String> dataStream = inputStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String data) throws Exception {
                String[] split = data.split(",");
                User user = User.of(Integer.parseInt(split[0]),
                        split[1],
                        Integer.parseInt(split[2]));
                return User.convertToCsv(user);
            }
        });

        // 写入到clickhouse
        Properties props = new Properties();
        props.put(ClickHouseSinkConst.TARGET_TABLE_NAME, "default.NewTable");
        props.put(ClickHouseSinkConst.MAX_BUFFER_SIZE, "10000");
        //    dataStream.addSink(new ClickHouseSink(props));
        dataStream.addSink(new SinkContext<>(new ClickHouseSink(props)).build());
        dataStream.print();

        env.execute("clickhouse sink test");
    }
}
