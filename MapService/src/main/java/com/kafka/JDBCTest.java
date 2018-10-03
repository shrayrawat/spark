package com.kafka;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JDBCTest {

	private final String topic;
	private final String bootstrapServers;
	private final String clientId;

	private Consumer<String, String> consumer;

	public JDBCTest(String bootstrapServers, String topic, String clientId) {
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

	public static void main(String[] args) {
		JDBCTest test = new JDBCTest("localhost:9092", "batch_demand_supply_topic", "batch_group");
		Map<String, String> records = null;
		JSONParser parser = new JSONParser();
		while (true) {
			try {
				records = test.getNewMessages();
				Connection conn = null;
				Statement stmt;
				String url = "jdbc:mysql://localhost:3306/batch_supply_demand_db";
				String username = "root";
				String password = "Intuit_18";
				conn = DriverManager.getConnection(url, username, password);
				stmt = conn.createStatement();
				// String str=
				// "{"geohash":"dr5rvv","timestamp":"1538564040000","supply":3,"demand":10,"weather":"T
				// : 17.37'C, H : 92%"}";
				String createTable = "CREATE  TABLE IF NOT EXISTS `batch_supply_demand_db`.`batch_demand_supply_table` (supply_demand_id INT NOT NULL AUTO_INCREMENT, geohash VARCHAR(100) NOT NULL, supply INT NOT NULL, demand INT NOT NULL, timestamp DATETIME, weather VARCHAR(40) NOT NULL, PRIMARY KEY ( supply_demand_id ) );";
				stmt.executeUpdate(createTable);
				for (Entry<String, String> entry : records.entrySet()) {
					String value = entry.getValue();
					JSONObject jsonObject = (JSONObject) parser.parse(value);
					String geoHash = (String) jsonObject.get("geohash");
					String timestamp = (String) jsonObject.get("timestamp");
					long supply = (Long) jsonObject.get("supply");
					long demand = (Long) jsonObject.get("demand");
					String weather = (String) jsonObject.get("weather");
					String query = "insert into batch_demand_supply_table(geohash,supply,demand,timestamp,weather) values('"
							+ geoHash + "'," + supply + "," + demand + ", from_unixtime(floor(" + timestamp
							+ "/1000)),\"" + weather + "\")";
					int set = stmt.executeUpdate(query);
				}
			} catch (InterruptedException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			} catch (IOException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			} catch (SQLException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}
			System.out.println("Number of records received : " + records.size());
			System.out.println(records);
		}
	}
}
