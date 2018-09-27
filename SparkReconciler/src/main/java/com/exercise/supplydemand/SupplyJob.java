package com.exercise.supplydemand;

import java.io.File;
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

import scala.Tuple2;

public class SupplyJob {
	static SparkSession spark;
	static JSONParser parser = new JSONParser();
	static Calendar cal = DateUtils.truncate(Calendar.getInstance(), Calendar.MINUTE);

	public static void readFromKafkaTopic() {
		spark = SparkSession.builder().config("spark.master", "local").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		JavaStreamingContext streamingContext = new JavaStreamingContext(
				JavaSparkContext.fromSparkContext(spark.sparkContext()), new Duration(10000));

//		String checkpointPath = File.separator + "tmp" + File.separator + "CAA2" + File.separator + "checkpoints2";
//		File checkpointDir = new File(checkpointPath);
//		checkpointDir.mkdir();
//		//checkpointDir.deleteOnExit();
//		streamingContext.checkpoint(checkpointPath);

		Map<String, Object> kafkaParams2 = new HashMap<>();
		kafkaParams2.put("bootstrap.servers", "localhost:9092");
		kafkaParams2.put("key.deserializer", StringDeserializer.class);
		kafkaParams2.put("value.deserializer", StringDeserializer.class);
		kafkaParams2.put("group.id", "use_a_separate_group_id_for_each_stream_2");
		kafkaParams2.put("auto.offset.reset", "latest");
		kafkaParams2.put("enable.auto.commit", false);
		Collection<String> topics2 = Arrays.asList(Constants.SUPPLY_TOPIC);

		final JavaInputDStream<ConsumerRecord<String, String>> stream2 = KafkaUtils.createDirectStream(streamingContext,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics2, kafkaParams2));

		JavaDStream<String> supplyLines = stream2.map(new Function<ConsumerRecord<String, String>, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(ConsumerRecord<String, String> kafkaRecord) throws Exception {
				return kafkaRecord.value();
			}
		});

		// supplyLines.print();

		JavaPairDStream<String, String> supplyPair = mapToPairSupply(supplyLines);
		reduceByKeyFunc(supplyPair);
		streamingContext.start();
		try {
			streamingContext.awaitTermination();
		} catch (InterruptedException e) {
		}
	}

	private static void reduceByKeyFunc(JavaPairDStream<String, String> supplyPair) {
		JavaPairDStream<String, Iterable<String>> supplyGroupedKeys = supplyPair.groupByKey();
		supplyGroupedKeys.print();
		supplyGroupedKeys.map(new Function<Tuple2<String, Iterable<String>>, String>() {

			@Override
			public String call(Tuple2<String, Iterable<String>> arg0) throws Exception {
				Set<String> supplySet = new HashSet<>();
				for (String str : arg0._2()) {
					supplySet.add(str);
				}
				System.out.println("supplySet: " + supplySet);
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
				String topic = "unified_topic";
				// producer.send(new ProducerRecord<String,
				// GenericTopicData>());
				producer.send(new ProducerRecord<String, byte[]>(topic,
						new GenericTopicData(arg0._1(), cal.getTime().getTime() + "", supplySet.size(), "S").toString()
								.getBytes()));
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
		SupplyJob.readFromKafkaTopic();
//		Thread sparkStreamingThread = new Thread(new Runnable() {
//
//			@Override
//			public void run() {
//				
//			}
//		});
//		sparkStreamingThread.start();
//		try {
//			// Sleep induced to give spark proper time to boot-up
//			Thread.sleep(10000);
//		} catch (InterruptedException e) {
//		}

		System.out.println("Ending thread.");

	}
}
