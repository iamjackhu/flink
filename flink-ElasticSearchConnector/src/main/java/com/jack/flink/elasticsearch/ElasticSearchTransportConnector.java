package com.jack.flink.elasticsearch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;

public class ElasticSearchTransportConnector {

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

		List<TransportAddress> transports = new ArrayList<TransportAddress>();
		transports.add(new InetSocketTransportAddress("es-node-1", 9300));
		transports.add(new InetSocketTransportAddress("es-node-2", 9300));
		transports.add(new InetSocketTransportAddress("es-node-3", 9300));

		input.addSink(new ElasticsearchSink(config, new IndexRequestBuilder<String>() {
			public IndexRequest createIndexRequest(String element, RuntimeContext ctx) {
				Map<String, Object> json = new HashMap<String, Object>();
				json.put("data", element);
				return Requests.indexRequest().index("my-index").type("my-type").source(json);
			}
		}));
	}

}
