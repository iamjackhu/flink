package com.jack.flink.cassandra;

import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer081;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import com.datastax.driver.core.Cluster;

public class CassandraConnector {

	public static void main(String[] args) {

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> input = env
				.addSource(new FlinkKafkaConsumer081<String>("topic_input", new SimpleStringSchema(), properties));

		try {
			CassandraSink.addSink(input).setQuery("INSERT INTO cep.events (id, message) values (?, ?);")
					.setClusterBuilder(new ClusterBuilder() {
						@Override
						public Cluster buildCluster(Cluster.Builder builder) {
							return builder.addContactPoint("127.0.0.1").build();
						}
					}).build();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
