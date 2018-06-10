package com.jack.flink.elasticsearch;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch.IndexRequestBuilder;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer081;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.collect.Maps;

public class ElasticSearchConnector {

	public static void main(String[] args) {

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> input = env
				.addSource(new FlinkKafkaConsumer081<String>("topic_input", new SimpleStringSchema(), properties));

		Map<String, String> config = Maps.newHashMap();
		config.put("bulk.flush.max.actions", "1");
		config.put("cluster.name", "cluster-name");

		input.addSink(new ElasticsearchSink(config, new IndexRequestBuilder<String>() {
			public IndexRequest createIndexRequest(String element, RuntimeContext ctx) {
				Map<String, Object> json = new HashMap<String, Object>();
				json.put("data", element);
				return Requests.indexRequest().index("my-index").type("my-type").source(json);
			}
		}));

	}

}
