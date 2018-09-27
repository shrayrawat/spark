package com.kafka.producer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.kafka.producer.utils.KafkaProducerInitUtils;

public class DemandKafkaProducer extends KafkaProducerInitUtils {
	private static String KAFKA_BROKER = "localhost:9092";
	private static String topic = "demand_topic";
	private static final long sleep = 101;
	private static long maxRows=200;
	private static String file = "/Users/srawat1/Desktop/traffic/testing_set/demand_data.csv";

	public static void main(String[] args) throws InterruptedException, ExecutionException {

//		if (args.length != 4) {
//			System.out.println(
//					"Usage : package.MainClass <data_file> <topic_name> <kafka_brokers> <rows(-1 to load all rows)>");
//			System.exit(1);
//		}

		// file = args[0];
		file = "/Users/srawat1/Desktop/traffic/testing_set/demand_data.csv";
		// topic = args[1];
		// KAFKA_BROKER = args[2];
		// maxRows = Long.parseLong(args[3]);
		maxRows = 200;
		if (maxRows < 0)
			maxRows = Long.MAX_VALUE;
		new DemandKafkaProducer().run();
	}

	public void run() throws ExecutionException, InterruptedException {

		System.out.println(String.format("Loading data from %s to %s kafka topic", file, topic));

		Producer<String, String> producer = getKafkaProducer(KAFKA_BROKER);
		long sent = 0;
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
			String sCurrentLine;
			while ((sCurrentLine = br.readLine()) != null) {
				producer.send(new ProducerRecord<String, String>(topic, sCurrentLine)).get();
				sent++;
				System.out.println("sent = " + sent);
				if (sent >= maxRows)
					break;
				Thread.sleep(sleep);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			System.out.println("Records pushed to kafka topic = " + sent);
			producer.close();
		}
	}
}
