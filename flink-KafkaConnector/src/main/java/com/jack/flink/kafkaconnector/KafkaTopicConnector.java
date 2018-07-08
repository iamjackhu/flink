package com.jack.flink.kafkaconnector;

import java.util.Properties;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer081;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

public class KafkaTopicConnector {

	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> stream = env
				.addSource(new FlinkKafkaConsumer081<String>("topic_input", new SimpleStringSchema(), properties));

		DataStream<WordWithCount> result = stream.rebalance().filter(new FilterFunction<String>() {
			public boolean filter(String value) throws Exception {
				System.out.println(Thread.currentThread().getId() + " - filter:" + value);
				return true;
			}
		}).flatMap(new FlatMapFunction<String, WordWithCount>() {
			public void flatMap(String value, Collector<WordWithCount> out) {
				System.out.println(Thread.currentThread().getId() + " - flatMap:" + value);
				for (String word : value.split(" ")) {
					out.collect(new WordWithCount(word, 1L));
				}
			}
		}).keyBy("word").timeWindow(Time.seconds(2), Time.seconds(1)).reduce(new ReduceFunction<WordWithCount>() {
			public WordWithCount reduce(WordWithCount a, WordWithCount b) {
				System.out.println(Thread.currentThread().getId() + " - reduce:" + a.word + ":" + a.count + ";" + b.word
						+ ":" + b.count);
				return new WordWithCount(a.word, a.count + b.count);
			}
		});

	}

	public static class WordWithCount {

		public String word;
		public long count;

		public WordWithCount() {
		}

		public WordWithCount(String word, long count) {
			this.word = word;
			this.count = count;
		}

		@Override
		public String toString() {
			return word + " : " + count;
		}
	}

}
