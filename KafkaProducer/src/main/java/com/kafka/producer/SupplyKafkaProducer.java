package com.kafka.producer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.kafka.producer.utils.KafkaProducerInitUtils;

public class SupplyKafkaProducer extends KafkaProducerInitUtils {
	private static String KAFKA_BROKER = "localhost:9092";
	private static long maxRows=200;
	private static final String topic = "supply_topic";
	private static final long sleep = 201;
	private static final String file = "/Users/srawat1/Desktop/traffic/testing_set/supply_data_traffic2.csv";

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		maxRows = 200;
		new SupplyKafkaProducer().run();
	}

	public void run() throws ExecutionException, InterruptedException {
		Producer<String, String> producer = getKafkaProducer(KAFKA_BROKER);
		long sent = 0;
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
			String sCurrentLine;
			while ((sCurrentLine = br.readLine()) != null) {
				producer.send(new ProducerRecord<String, String>(topic, sCurrentLine)).get();
				sent++;
				if (sent >= maxRows)
					// if (sent == 200)
					break;
				Thread.sleep(sleep);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			System.out.println("Records pushed to kafka = " + sent);
			producer.close();
		}
	}
}
