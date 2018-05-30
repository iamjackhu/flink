package com.jack.flink.twitter;

import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

public class TwitterConnector {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.setProperty(TwitterSource.CONSUMER_KEY, "");
		props.setProperty(TwitterSource.CONSUMER_SECRET, "");
		props.setProperty(TwitterSource.TOKEN, "");
		props.setProperty(TwitterSource.TOKEN_SECRET, "");

		// get the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> streamSource = env.addSource(new TwitterSource(props));
		
	}

}
