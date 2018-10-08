package com.exercise.spark.batch;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
import org.apache.spark.api.java.function.PairFlatMapFunction;
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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.commons.model.BatchRecord;
import com.commons.util.WeatherUtil;
import com.exercise.models.GenericTopicData;
import com.exercise.util.Constants;
import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;

public class SupplyDemandBatchJob {
	static SparkSession spark;
	static JSONParser parser = new JSONParser();

	public static void readFromKafkaTopic() {
		spark = SparkSession.builder().config("spark.master", "local")
				.config("spark.streaming.backpressure.enabled", "true")
				.config("spark.streaming.kafka.maxRatePerPartition", 500)
				.config("spark.streaming.backpressure.initialRate", 500).getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		JavaStreamingContext streamingContext = new JavaStreamingContext(
				JavaSparkContext.fromSparkContext(spark.sparkContext()), new Duration(240000));

		String checkpointPath = File.separator + "tmp" + File.separator + "CAA44" + File.separator + "checkpoints4";
		File checkpointDir = new File(checkpointPath);
		checkpointDir.mkdir();
		checkpointDir.deleteOnExit();
		streamingContext.checkpoint(checkpointPath);

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "batch_job");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		Collection<String> topics = Arrays.asList(Constants.COMMON_TOPIC_BATCH);

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
				return kafkaRecord.value().toString();
			}
		});
		lines.print();
		JavaPairDStream<String, GenericTopicData> geoToUserData = createGeoHashToUserDataPair(lines);
		reduceByKeyFunc(geoToUserData);
		streamingContext.start();
		try {
			streamingContext.awaitTermination();
		} catch (InterruptedException e) {
		}
	}

	private static void reduceByKeyFunc(JavaPairDStream<String, GenericTopicData> geoToUserData) {
		/*
		 * Group multiple records according to geohash
		 */
		JavaPairDStream<String, Iterable<GenericTopicData>> supplyGroupedKeys = geoToUserData.groupByKey();
		supplyGroupedKeys.print();

		JavaPairDStream<String, GenericTopicData> segregatedAcctoTsList = supplyGroupedKeys.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String, Iterable<GenericTopicData>>, String, GenericTopicData>() {

					@Override
					public Iterator<Tuple2<String, GenericTopicData>> call(
							Tuple2<String, Iterable<GenericTopicData>> arg0) throws Exception {
						List<Tuple2<String, GenericTopicData>> list = new ArrayList<>();
						for (GenericTopicData data : arg0._2()) {
							Tuple2<String, GenericTopicData> tuple = new Tuple2<String, GenericTopicData>(
									arg0._1 + "_" + data.getTimestamp(), data);
							list.add(tuple);
						}
						return list.iterator();
					}
				});

		segregatedAcctoTsList.print();

		segregatedAcctoTsList.groupByKey().print();

		segregatedAcctoTsList.groupByKey().map(new Function<Tuple2<String, Iterable<GenericTopicData>>, String>() {

			@Override
			public String call(Tuple2<String, Iterable<GenericTopicData>> arg0) throws Exception {
				int supply = 0;
				int demand = 0;
				System.out.println(arg0._1);
				Set<String> supplySet = new HashSet<>();
				Set<String> demandSet = new HashSet<>();
				for (GenericTopicData data : arg0._2()) {
					if (data.getRequestName().equals("S")) {
						supplySet.addAll(data.getRequestSet());
					} else if (data.getRequestName().equals("D")) {
						demandSet.addAll(data.getRequestSet());
					}
				}
				supply = supplySet.size();
				demand = demandSet.size();
				String weatherInfo = WeatherUtil.getWeatherInfo(arg0._1);
				Calendar cal = DateUtils.truncate(Calendar.getInstance(), Calendar.MINUTE);
				BatchRecord record = new BatchRecord(arg0._1.split("_")[0], cal.getTime().getTime() + "", supply,
						demand, weatherInfo);
				ObjectMapper mapper = new ObjectMapper();
				String jsonInString = mapper.writeValueAsString(record);
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
				String topic = Constants.OUTPUT_TOPIC_BATCH;
				// producer.send(new ProducerRecord<String,
				// GenericTopicData>());
				producer.send(new ProducerRecord<String, byte[]>(topic, arg0._1.split("_")[0],
						jsonInString.toString().getBytes()));
				return "";

			}
		}).count().print();

		// supplyGroupedKeys.map(new Function<Tuple2<String,
		// Iterable<GenericTopicData>>, String>() {
		//
		// @Override
		// public String call(Tuple2<String, Iterable<GenericTopicData>> arg0)
		// throws Exception {
		// int supply = 0;
		// int demand = 0;
		// System.out.println(arg0._1);
		// for (GenericTopicData data : arg0._2()) {
		// if (data.getRequestName().equals("S")) {
		// supply = supply + data.getRequestSize();
		// } else if (data.getRequestName().equals("D")) {
		// demand = demand + data.getRequestSize();
		// }
		// }
		// String weatherInfo = WeatherUtil.getWeatherInfo(arg0._1);
		// BatchRecord record = new BatchRecord(arg0._1, cal.getTime().getTime()
		// + "", supply, demand,
		// weatherInfo);
		// ObjectMapper mapper = new ObjectMapper();
		// String jsonInString = mapper.writeValueAsString(record);
		// Properties props = new Properties();
		// props.put("bootstrap.servers", "localhost:9092");
		// props.put("acks", "all");
		// props.put("retries", 0);
		// props.put("batch.size", 16384);
		// props.put("linger.ms", 1);
		// props.put("request.timeout.ms", 60000);
		// props.put("buffer.memory", 33554432);
		// props.put("key.serializer",
		// "org.apache.kafka.common.serialization.StringSerializer");
		// props.put("value.serializer",
		// "org.apache.kafka.common.serialization.ByteArraySerializer");
		// Producer<String, byte[]> producer = new KafkaProducer(props);
		// String topic = "batch_demand_supply_topic";
		// // producer.send(new ProducerRecord<String,
		// // GenericTopicData>());
		// producer.send(new ProducerRecord<String, byte[]>(topic, arg0._1,
		// jsonInString.toString().getBytes()));
		// return "";
		//
		// }
		// }).count().print();

	}

	private static JavaPairDStream<String, GenericTopicData> createGeoHashToUserDataPair(JavaDStream<String> lines) {
		JavaPairDStream<String, GenericTopicData> pair = lines
				.mapToPair(new PairFunction<String, String, GenericTopicData>() {

					@Override
					public Tuple2<String, GenericTopicData> call(String arg0) throws Exception {
						JSONObject jsonObject = (JSONObject) parser.parse(arg0);
						String geoHash = (String) jsonObject.get("geoHash");
						long requestSize = (long) jsonObject.get("requestSize");
						String timestamp = (String) jsonObject.get("timestamp");
						String requestName = (String) jsonObject.get("requestName");
						Set<String> requestSet = new HashSet<String>((JSONArray) jsonObject.get("requestSet"));
						return new Tuple2<String, GenericTopicData>(geoHash,
								new GenericTopicData(geoHash, timestamp, (int) requestSize, requestName, requestSet));
					}
				});
		pair.print();
		return pair;
	}

	public static void main(String[] args) {
		SupplyDemandBatchJob.readFromKafkaTopic();
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
