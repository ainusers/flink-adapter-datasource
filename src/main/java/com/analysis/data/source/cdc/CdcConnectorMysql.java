package com.analysis.data.source.cdc;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * @author: tianyong
 * @time: 2021/9/27 15:45
 * @description:
 * @Version: v1.0
 * @company: xxx Group.Situation xxx事业部
 */
public class CdcConnectorMysql {
    public static void main(String[] args) throws Exception {
        SourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("192.168.43.43")
                .port(3306)
                .databaseList("test")
                .tableList("test.test")
                .username("root")
                .password("root1234")
                .startupOptions(StartupOptions.latest())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.addSource(sourceFunction);

        // 数据发送到kafka
        dataStream.addSink(new FlinkKafkaProducer<String>("10.43.80.80:9092","sink-kafka", new SimpleStringSchema()));

        dataStream.print();

        env.execute();
    }
}

