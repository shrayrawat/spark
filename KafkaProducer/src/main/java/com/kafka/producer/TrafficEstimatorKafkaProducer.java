package com.kafka.producer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.kafka.producer.utils.KafkaProducerInitUtils;

public class TrafficEstimatorKafkaProducer extends KafkaProducerInitUtils {
	private static String KAFKA_BROKER = "localhost:9092";
	private static final String topic = "supply_topic2";
	private static final long sleep = 10l;
	private static final String file = "";

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		new TrafficEstimatorKafkaProducer().run();
	}

	public void run() throws ExecutionException, InterruptedException {
		Producer<String, String> producer = getKafkaProducer(KAFKA_BROKER);
		long sent = 0;
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
			String sCurrentLine;
			while ((sCurrentLine = br.readLine()) != null) {
				producer.send(new ProducerRecord<String, String>(topic, sCurrentLine)).get();
				sent++;
				if (sent == 3000)
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
