
package com.jack.flink.wordcount;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


public class SocketWindowWordCount {
	public static void main(String[] args) throws Exception {
		//
		// Map<String, String> config = Maps.newHashMap();
		// config.put("bulk.flush.max.actions", "1");
		// config.put("cluster.name", "elasticsearch_jack");

		// the port to connect to
		final int port;
		try {
			final ParameterTool params = ParameterTool.fromArgs(args);
			port = params.getInt("port");
		} catch (Exception e) {
			System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'");
			return;
		}

		// get the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// get input data by connecting to the socket
		DataStream<String> text = env.socketTextStream("localhost", port, "\n");

		// parse the data, group it, window it, and aggregate the counts
		DataStream<WordWithCount> windowCounts = text.rebalance().filter(new FilterFunction<String>() {
			public boolean filter(String value) throws Exception {
				System.out.println(Thread.currentThread().getId() + " - filter:" + value);
				return !value.startsWith("s");
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

		// print the results with a single thread, rather than in parallel
		windowCounts.print().setParallelism(1);

		// windowCounts.addSink(new ElasticsearchSink<>(config, new
		// IndexRequestBuilder<String>() {
		// @Override
		// public IndexRequest createIndexRequest(String element, RuntimeContext ctx) {
		// Map<String, Object> json = new HashMap<>();
		// json.put("data", element);
		//
		// return Requests.indexRequest()
		// .index("my-index")
		// .type("my-type")
		// .source(json);
		// }
		// }));

		env.execute("Socket Window WordCount");
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
