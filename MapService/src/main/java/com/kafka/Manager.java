package com.kafka;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.models.LocationHolder;
import com.service.util.Utilities;

enum Topics {
	final_demand_supply_topic, traffic_output;
}

public class Manager {

	private static Manager INSTANCE = new Manager();
	Map<String, Poller> kafkaPollers = new HashMap();
	private String kafkaBroker;

	private Manager() {
	}

	public static Manager get() {
		return INSTANCE;
	}

	public void init(String kafkaBroker) {
		System.out.println("Initialized with kafkabroker : " + kafkaBroker);
		this.kafkaBroker = kafkaBroker;
	}

	public LocationHolder getNextMessage(String topic, String clientId) throws IOException, InterruptedException {

		String key = topic + "." + clientId;
		if (!kafkaPollers.containsKey(key)) {
			System.out.println("Key Does not Exists  = " + key);
			kafkaPollers.put(key, new Poller(kafkaBroker, topic, clientId));
		} else {
			System.out.println("Key Exists  = " + key);
		}

		Map<String, String> records = kafkaPollers.get(key).getNewMessages();
		System.out.println("Number of records received : " + records.size());

		if (topic.equals(Topics.final_demand_supply_topic.toString()))
			return Utilities.toSupplyDemandGeoPoint(records);
		if (topic.equals(Topics.traffic_output.toString()))
			return Utilities.toTrafficGeoPoint(records);
		return null;
	}

	public void reset(String topic, String clientId) throws IOException, InterruptedException {
		String key = topic + "." + clientId;
		if (kafkaPollers.containsKey(key)) {
			kafkaPollers.get(key).reset();
			System.out.println("Kafka consumer for the following key is reset :  " + key);
		} else {
			System.out.println("Kafka consumer does not exist for key :  " + key);
		}
	}
}
