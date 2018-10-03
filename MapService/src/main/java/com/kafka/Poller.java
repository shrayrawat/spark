package com.kafka;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Poller {

	private final String topic;
	private final String bootstrapServers;
	private final String clientId;

	private Consumer<String, String> consumer;

	public Poller(String bootstrapServers, String topic, String clientId) {
		this.bootstrapServers = bootstrapServers;
		this.topic = topic;
		this.clientId = clientId;
		init();
	}

	private void init() {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaPoller-" + topic + "-" + clientId);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		// Create the consumer using props.
		final Consumer<String, String> consumer = new KafkaConsumer(props);
		// Subscribe to the topic.
		consumer.subscribe(Collections.singletonList(topic));
		consumer.poll(0);
		TopicPartition tp = new TopicPartition(topic, 0);
		consumer.seekToBeginning(Arrays.asList(tp));
		System.out.println("consumer.position(tp) = " + consumer.position(tp));
		this.consumer = consumer;
	}

	public void reset() {
		System.out.println("Resetting kafka consumer for " + topic);
		TopicPartition tp = new TopicPartition(topic, 0);
		consumer.seekToBeginning(Arrays.asList(tp));
		System.out.println("Kafka consumer position after reset = " + consumer.position(tp));
	}

	public Map<String, String> getNewMessages() throws InterruptedException, IOException {
		Map<String, String> records = new HashMap();
		final ConsumerRecords<String, String> consumerRecords = consumer.poll(500);
		if (consumerRecords.count() >= 0) {
			for (ConsumerRecord<String, String> record : consumerRecords) {
				String key = record.key();
				String json = record.value();
				records.put(key, json);
			}
		}
		consumer.commitAsync();
		System.out.println("Polled records = " + records.size());
		return records;
	}

	public void close() {
		consumer.close();
	}
}
