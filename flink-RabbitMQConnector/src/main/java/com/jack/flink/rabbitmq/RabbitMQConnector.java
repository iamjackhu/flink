package com.jack.flink.rabbitmq;

import java.io.IOException;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

public class RabbitMQConnector {

	public static void main(String[] args) {
		RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder().setHost("localhost").setPort(5678)
				.setUserName("rabbitmq").setPassword("rabbitma").setVirtualHost("/").build();

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Get Data Stream without correlation ids
		DataStream<String> streamWO = env
				.addSource(new RMQSource<String>(connectionConfig, "my-queue", new DeserializationSchema() {

					public TypeInformation getProducedType() {
						// TODO Auto-generated method stub
						return null;
					}

					public Object deserialize(byte[] message) throws IOException {
						// TODO Auto-generated method stub
						return null;
					}

					public boolean isEndOfStream(Object nextElement) {
						// TODO Auto-generated method stub
						return false;
					}

				}));
		// Get Data Stream with correlation ids
		DataStream<String> streamW = env
				.addSource(new RMQSource<String>(connectionConfig, "my-queue", true, new DeserializationSchema() {

					public TypeInformation getProducedType() {
						// TODO Auto-generated method stub
						return null;
					}

					public Object deserialize(byte[] message) throws IOException {
						// TODO Auto-generated method stub
						return null;
					}

					public boolean isEndOfStream(Object nextElement) {
						// TODO Auto-generated method stub
						return false;
					}

				}));
		
		
		streamWO.addSink(new RMQSink<String>(connectionConfig, "target-queue", new SerializationSchema() {

			public byte[] serialize(Object element) {
				// TODO Auto-generated method stub
				return null;
			}
			
		})); 
	}

}
