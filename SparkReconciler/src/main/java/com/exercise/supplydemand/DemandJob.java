package com.exercise.supplydemand;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.time.DateUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.commons.util.GeoHashUtil;
import com.exercise.models.GenericTopicData;
import com.exercise.util.Constants;
import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;

public class DemandJob {
	static SparkSession spark;
	static JSONParser parser = new JSONParser();
	static Calendar cal = DateUtils.truncate(Calendar.getInstance(), Calendar.MINUTE);

	public static void readFromKafkaTopic() {
		spark = SparkSession.builder().config("spark.master", "local")
				.config("spark.streaming.backpressure.enabled", "true")
				.config("spark.streaming.kafka.maxRatePerPartition", 50).getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		JavaStreamingContext streamingContext = new JavaStreamingContext(
				JavaSparkContext.fromSparkContext(spark.sparkContext()), new Duration(5000));

		// String checkpointPath = File.separator + "tmp" + File.separator +
		// "CAA" + File.separator + "checkpoints";
		// File checkpointDir = new File(checkpointPath);
		// checkpointDir.mkdir();
		// //checkpointDir.deleteOnExit();
		// streamingContext.checkpoint(checkpointPath);

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		Collection<String> topics = Arrays.asList(Constants.DEMAND_TOPIC);

		final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(streamingContext,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		JavaDStream<String> lines = stream.map(new Function<ConsumerRecord<String, String>, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(ConsumerRecord<String, String> kafkaRecord) throws Exception {
				return kafkaRecord.value();
			}
		});

		JavaPairDStream<String, String> demandPair = mapToPairDemand(lines);
		reduceByKeyFunc(demandPair);
		streamingContext.start();
		try {
			streamingContext.awaitTermination();
		} catch (InterruptedException e) {
		}
	}

	private static void reduceByKeyFunc(JavaPairDStream<String, String> demandPair) {
		JavaPairDStream<String, Iterable<String>> demandGroupedKeys = demandPair.groupByKey();
		demandGroupedKeys.print();
		demandGroupedKeys.map(new Function<Tuple2<String, Iterable<String>>, String>() {

			@Override
			public String call(Tuple2<String, Iterable<String>> arg0) throws Exception {
				Set<String> demandSet = new HashSet<>();
				for (String str : arg0._2()) {
					demandSet.add(str);
				}
				System.out.println("demandSet: " + demandSet);
				Properties props = new Properties();
				props.put("bootstrap.servers", "localhost:9092");
				props.put("acks", "all");
				props.put("retries", 0);
				props.put("batch.size", 16384);
				props.put("linger.ms", 1);
				props.put("request.timeout.ms", 60000);
				props.put("buffer.memory", 33554432);
				props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
				props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
				Producer<String, byte[]> producer = new KafkaProducer(props);
				String topic = Constants.COMMON_TOPIC_RT;
				ObjectMapper mapper = new ObjectMapper();
				String jsonInString = mapper.writeValueAsString(new GenericTopicData(arg0._1(),
						cal.getTime().getTime() + "", demandSet.size(), "D", demandSet));
				producer.send(new ProducerRecord<String, byte[]>(topic, jsonInString.getBytes()));
				return "";

			}
		}).print();

	}

	private static JavaPairDStream<String, String> mapToPairSupply(JavaDStream<String> lines) {
		JavaPairDStream<String, String> pair = lines.mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String arg0) throws Exception {
				JSONObject jsonObject = (JSONObject) parser.parse(arg0);

				double lat = (Double) jsonObject.get("curr_latitude");
				double lon = (Double) jsonObject.get("curr_longitude");
				String geoHash = GeoHashUtil.toGeohash(lat, lon);

				String customerId = (String) jsonObject.get("driver_id");

				return new Tuple2<String, String>(geoHash, customerId);
			}
		});

		return pair;
	}

	private static JavaPairDStream<String, String> mapToPairDemand(JavaDStream<String> lines) {
		JavaPairDStream<String, String> pair = lines.mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String arg0) throws Exception {
				JSONObject jsonObject = (JSONObject) parser.parse(arg0);

				double lat = (Double) jsonObject.get("curr_latitude");
				double lon = (Double) jsonObject.get("curr_longitude");
				String geoHash = GeoHashUtil.toGeohash(lat, lon);

				String customerId = (String) jsonObject.get("customer_id");

				return new Tuple2<String, String>(geoHash, customerId);
			}
		});
		return pair;
	}

	public static void main(String[] args) {
		DemandJob.readFromKafkaTopic();
		// Thread sparkStreamingThread = new Thread(new Runnable() {
		//
		// @Override
		// public void run() {
		//
		// }
		// });
		// sparkStreamingThread.start();
		// try {
		// // Sleep induced to give spark proper time to boot-up
		// Thread.sleep(10000);
		// } catch (InterruptedException e) {
		// }

		System.out.println("Ending thread.");

	}
}
