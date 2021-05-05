package com.flink.kafka.sink.demo;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class FlinkKafkaConsumerDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(
                java.util.regex.Pattern.compile("quickstart-events"),
                new SimpleStringSchema(),
                properties);

        FlinkKafkaProducer<String> kafkaProducer =
                new FlinkKafkaProducer<String>("quickstart-events-output",
                        ((record, timestamp) ->
                                new ProducerRecord<byte[], byte[]>("quickstart-events-output",
                                        record.getBytes(), record.getBytes())),
                        properties,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        DataStream<String> stream = env.addSource(myConsumer);

        stream.addSink(kafkaProducer);

        env.execute();

    }
}
