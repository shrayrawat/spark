package com.kafka.producer.utils;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;

public class KafkaProducerInitUtils {
	public KafkaProducer getKafkaProducer(String broker) {
		Properties props = new Properties();
		props.put("bootstrap.servers", broker);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("request.timeout.ms", 60000);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return new KafkaProducer(props);
	}
}
