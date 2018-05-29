package com.jack.flink.kafkaconnector;

import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer081;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class KafkaTopicConnector {

	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		DataStream<String> stream = env
				.addSource(new FlinkKafkaConsumer081<String>("topic_input", new SimpleStringSchema(), properties));
		
		stream.addSink(new FlinkKafkaProducer("localhost:9092", "topic_out", new SimpleStringSchema()));
		
	}

}
