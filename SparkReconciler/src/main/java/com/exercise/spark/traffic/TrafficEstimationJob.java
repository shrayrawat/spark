package com.exercise.spark.traffic;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.lang.time.DateUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
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

import com.commons.model.DriverPing;
import com.commons.model.LatAndLong;
import com.commons.model.TrafficData;
import com.commons.util.WeatherUtil;
import com.exercise.util.Constants;
import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;

public class TrafficEstimationJob {

	static SparkSession spark;
	static JSONParser parser = new JSONParser();
	static Calendar cal = DateUtils.truncate(Calendar.getInstance(), Calendar.MINUTE);

	public static void readFromKafkaTopic() {
		spark = SparkSession.builder().config("spark.master", "local").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		JavaStreamingContext streamingContext = new JavaStreamingContext(
				JavaSparkContext.fromSparkContext(spark.sparkContext()), new Duration(240000));

		// String checkpointPath = File.separator + "tmp" + File.separator +
		// "CAA2" + File.separator + "checkpoints2";
		// File checkpointDir = new File(checkpointPath);
		// checkpointDir.mkdir();
		// //checkpointDir.deleteOnExit();
		// streamingContext.checkpoint(checkpointPath);

		Map<String, Object> kafkaParams2 = new HashMap<>();
		kafkaParams2.put("bootstrap.servers", "localhost:9092");
		kafkaParams2.put("key.deserializer", StringDeserializer.class);
		kafkaParams2.put("value.deserializer", StringDeserializer.class);
		kafkaParams2.put("group.id", "use_a_separate_group_id_for_each_stream_5");
		kafkaParams2.put("auto.offset.reset", "latest");
		kafkaParams2.put("enable.auto.commit", false);
		Collection<String> topics2 = Arrays.asList(Constants.SUPPLY_TOPIC_TRAFFIC);

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

		supplyLines.print();

		JavaPairDStream<String, DriverPing> supplyPair = mapToPairSupply(supplyLines);

		convertToDriverWithPingListPair(supplyPair);

		// reduceByKeyFunc(supplyPair);
		streamingContext.start();
		try {
			streamingContext.awaitTermination();
		} catch (InterruptedException e) {
		}
	}

	private static void convertToDriverWithPingListPair(JavaPairDStream<String, DriverPing> supplyPair) {

		JavaPairDStream<String, Iterable<DriverPing>> supplyGroupedKeys = supplyPair.groupByKey();
		supplyGroupedKeys.print();
		JavaDStream<Map<String, List<Double>>> geohashWithListOfAvgSpeeds = supplyGroupedKeys
				.map(new Function<Tuple2<String, Iterable<DriverPing>>, Map<String, List<Double>>>() {

					@Override
					public Map<String, List<Double>> call(Tuple2<String, Iterable<DriverPing>> arg0) throws Exception {
						List<DriverPing> list = new ArrayList<DriverPing>();
						for (DriverPing item : arg0._2) {
							list.add(item);
						}
						return TrafficUtility.getSpeedValues(list);
					}
				});
		geohashWithListOfAvgSpeeds.print();
		JavaDStream<Map<String, List<Double>>> mergedMap = geohashWithListOfAvgSpeeds.reduce(
				new Function2<Map<String, List<Double>>, Map<String, List<Double>>, Map<String, List<Double>>>() {

					@Override
					public Map<String, List<Double>> call(Map<String, List<Double>> arg0,
							Map<String, List<Double>> arg1) throws Exception {
						return TrafficUtility.merge(arg0, arg1);
					}
				});

		mergedMap.foreachRDD(new VoidFunction<JavaRDD<Map<String, List<Double>>>>() {

			@Override
			public void call(JavaRDD<Map<String, List<Double>>> arg0) throws Exception {
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
				String topic = Constants.TRAFFIC_OUTPUT_TOPIC;

				Iterator<Map<String, List<Double>>> iter = arg0.toLocalIterator();
				while (iter.hasNext()) {
					Map<String, List<Double>> map = (Map<String, List<Double>>) iter.next();
					System.out.println("trafficSet: " + map);
					for (Entry<String, List<Double>> entry : map.entrySet()) {
						String weatherInfo = WeatherUtil.getWeatherInfo(entry.getKey());
						double avgSpeed = TrafficUtility.computeMean(entry.getValue());
						double normalizedSpeed = TrafficUtility.normalize(avgSpeed);
						TrafficData data = new TrafficData(entry.getKey(), cal.getTime().getTime(), avgSpeed,
								normalizedSpeed, weatherInfo);
						ObjectMapper mapper = new ObjectMapper();
						String jsonInString = mapper.writeValueAsString(data);
						producer.send(new ProducerRecord<String, byte[]>(topic, jsonInString.toString().getBytes()));
					}

				}

			}
		});

	}

	private static JavaPairDStream<String, DriverPing> mapToPairSupply(JavaDStream<String> lines) {
		JavaPairDStream<String, DriverPing> pair = lines.mapToPair(new PairFunction<String, String, DriverPing>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, DriverPing> call(String arg0) throws Exception {
				Calendar cal = DateUtils.truncate(Calendar.getInstance(), Calendar.MINUTE);
				DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				JSONObject jsonObject = (JSONObject) parser.parse(arg0);
				double lat = (Double) jsonObject.get("curr_latitude");
				double lon = (Double) jsonObject.get("curr_longitude");
				String driverId = (String) jsonObject.get("driver_id");
				Long timestamp = formatter.parse((String) jsonObject.get("timestamp")).getTime();
				LatAndLong latLong = new LatAndLong(lat, lon);
				DriverPing ping = new DriverPing(driverId, latLong, timestamp);
				return new Tuple2<String, DriverPing>(driverId, ping);
			}
		});

		return pair;
	}

	public static void main(String[] args) {
		TrafficEstimationJob.readFromKafkaTopic();
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
