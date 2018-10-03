package com.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.commons.util.GeoHashUtil;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.davidmoten.geo.LatLong;

public class DataUtilV2 {

	private static final String SEPARATOR = ",";
	private static final String ID_PREFIX = "CT";
	private static final Long INITIAL_ID = 77000l;
	private static final String OUTPUT_FILE = "/Users/srawat1/Desktop/traffic/demand_data_v3.csv";
	private static final String INPUT_FILE = "/Users/srawat1/Desktop/traffic/test_set3";

	public static final String ID_KEY = "customer_id";
	private static Map<String, List<String>> geoMap = new HashMap();
	static final double rangeMin = 0.3;
	static final double rangeMax = 2.0;

	public static void main(String[] args) throws JsonProcessingException {
		List<String> rows = new Reader().read(INPUT_FILE);
		List<String> transformed = transform(rows);

		System.out.println("transformed = " + transformed.size());
		System.out.println("geoSet = " + geoMap.size());
		new Writer().write(OUTPUT_FILE, transformed);
	}

	private static List<String> transform(List<String> geohashList) throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		List<String> transformed = new ArrayList();
		BaseRecord record;
		int i = 1;
		long initialTime = System.currentTimeMillis();
		for (String geohash : geohashList) {
			long current = initialTime + (long) (20000 * Math.random());
			List<LatLong> latLongs = extractLatLong(geohash);
			int iterations = (int) (getRandomDouble() * latLongs.size());
			for (int k = 0; k < iterations; k++) {
				String custId = ID_PREFIX + (INITIAL_ID + (++i));
				LatLong ll = latLongs.get(k % latLongs.size());
				record = new BaseRecord(custId, formatter.format(new Date(current += 3000)), ll.getLat(), ll.getLon());
				try {
					addToGeoMap(record.getCurr_latitude(), record.getCurr_longitude());
					String json = mapper.writeValueAsString(record);
					transformed.add(json);

					if (Math.random() > 0.40)
						transformed.add(json);

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return transformed;
	}

	private static double getRandomDouble() {
		Random r = new Random();
		return rangeMin + (rangeMax - rangeMin) * r.nextDouble();
	}

	private static void addToGeoMap(Double lat, Double lon) {
		String geoHash = GeoHashUtil.toGeohash(lat, lon);
		if (!geoMap.containsKey(geoHash)) {
			geoMap.put(geoHash, new ArrayList());
		}
		geoMap.get(geoHash).add(String.format("%s|%s", lat, lon));
	}

	private static List<LatLong> extractLatLong(String geohash) {
		System.out.println("geohash = " + geohash);
		String ll = geohash.split("\\^")[1];
		String[] lls = ll.substring(1, ll.length() - 1).split(",");

		List<LatLong> list = new ArrayList();
		for (String ll2 : lls) {
			String[] tokens = ll2.trim().split("\\|");
			list.add(new LatLong(Double.parseDouble(tokens[0]), Double.parseDouble(tokens[1])));
		}
		return list;
	}
}

class Reader {

	public List<String> read(String file) {

		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
			String sCurrentLine;
			List<String> rows = new ArrayList();
			int i = 1;
			while ((sCurrentLine = br.readLine()) != null) {
				// if (i++ == 1) {
				// continue;
				// }
				//
				// if(i==500)
				// {
				// break;
				// }

				rows.add(sCurrentLine);
			}
			System.out.println("Rows read : " + rows.size());
			return rows;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
}

class Writer {

	public void write(String file, List<String> rows) {
		System.out.println("Writing to file now...");
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
			for (String row : rows) {
				bw.write(row);
				bw.newLine();
			}
			System.out.println("Rows Written : " + rows.size());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}

class BaseRecord {

	@JsonProperty(DataUtilV2.ID_KEY)
	private String id;
	@JsonProperty("timestamp")
	private String timestamp;
	@JsonProperty("curr_latitude")
	private Double curr_latitude;
	@JsonProperty("curr_longitude")
	private Double curr_longitude;

	public BaseRecord(String id, String timestamp, Double curr_latitude, Double curr_longitude) {
		this.id = id;
		this.timestamp = timestamp;
		this.curr_latitude = curr_latitude;
		this.curr_longitude = curr_longitude;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public Double getCurr_latitude() {
		return curr_latitude;
	}

	public void setCurr_latitude(Double curr_latitude) {
		this.curr_latitude = curr_latitude;
	}

	public Double getCurr_longitude() {
		return curr_longitude;
	}

	public void setCurr_longitude(Double curr_longitude) {
		this.curr_longitude = curr_longitude;
	}
}
