//package com.util;
//
//import java.text.DateFormat;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import com.commons.util.GeoHashUtil;
//import com.fasterxml.jackson.annotation.JsonIgnore;
//import com.fasterxml.jackson.annotation.JsonProperty;
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//
//public class TrafficDataUtil extends DataUtil {
//
//	private static final String SEPARATOR = ",";
//	private static final String ID_PREFIX = "DR";
//	private static final Long INITIAL_ID = 22000l;
//	private static final String OUTPUT_FILE = "/Users/srawat1/Desktop/traffic/supply_data_traffic2.csv";
//	private static final String INPUT_FILE = "/Users/srawat1/Desktop/traffic/green_tripdata_2018-01.csv";
//	public static final String ID_KEY = "driver_id";
//	private static Map<String, List<String>> geoMap = new HashMap();
//	private static final int totalDrivers = 5;
//
//	public static void main(String[] args) throws JsonProcessingException {
//		List<String> rows = new Reader().read(INPUT_FILE);
//		List<String> transformed = transform(rows);
//		System.out.println("transformed = " + transformed.size());
//		System.out.println("geoMap = " + geoMap.size());
//
//		int count = 0;
//		for (String key : geoMap.keySet()) {
//			if (geoMap.get(key).size() > 5) {
//				System.out.println(String.format("%s^%s", key, geoMap.get(key)));
//				// System.out.println("\""+key+"\",");
//			}
//		}
//
//		new Writer().write(OUTPUT_FILE, transformed);
//	}
//
//	public static List<String> transform(List<String> rows) throws JsonProcessingException {
//
//		ObjectMapper mapper = new ObjectMapper();
//		Long Id = new Long(INITIAL_ID);
//
//		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//		Long current = System.currentTimeMillis();
//
//		List<String> transformed = new ArrayList();
//		TrafficBaseRecord record;
//		int i = 0;
//		for (String row : rows) {
//			if (row.trim().isEmpty())
//				continue;
//			String[] tokens = row.split(SEPARATOR);
//			record = new TrafficBaseRecord(ID_PREFIX + (INITIAL_ID + (++Id % totalDrivers)),
//					formatter.format(new Date(current += 1000)), Double.parseDouble(tokens[6]),
//					Double.parseDouble(tokens[5]), Double.parseDouble(tokens[8]), Double.parseDouble(tokens[7]));
//			try {
//				addToGeoMap(record.getCurr_latitude(), record.getCurr_longitude());
//				String json = mapper.writeValueAsString(record);
//				transformed.add(json);
//			} catch (Exception e) {
//				System.out.println("Invalid Lat-long : " + row);
//			}
//		}
//		return transformed;
//	}
//
//	private static void addToGeoMap(Double lat, Double lon) {
//		String geoHash = GeoHashUtil.toGeohash(lat, lon);
//		if (!geoMap.containsKey(geoHash)) {
//			geoMap.put(geoHash, new ArrayList());
//		}
//		geoMap.get(geoHash).add(String.format("%s|%s", lat, lon));
//	}
//}
//
//class TrafficBaseRecord {
//
//	@JsonProperty(TrafficDataUtil.ID_KEY)
//	private String id;
//	@JsonProperty("timestamp")
//	private String timestamp;
//	@JsonProperty("curr_latitude")
//	private Double curr_latitude;
//	@JsonProperty("curr_longitude")
//	private Double curr_longitude;
//	// @JsonProperty("last_latitude")
//	@JsonIgnore
//	private Double last_latitude;
//	// @JsonProperty("last_longitude")
//	@JsonIgnore
//	private Double last_longitude;
//
//	public TrafficBaseRecord(String id, String timestamp, Double curr_latitude, Double curr_longitude,
//			Double last_latitude, Double last_longitude) {
//		this.id = id;
//		this.timestamp = timestamp;
//		this.curr_latitude = curr_latitude;
//		this.curr_longitude = curr_longitude;
//		this.last_latitude = last_latitude;
//		this.last_longitude = last_longitude;
//	}
//
//	public String getId() {
//		return id;
//	}
//
//	public void setId(String id) {
//		this.id = id;
//	}
//
//	public String getTimestamp() {
//		return timestamp;
//	}
//
//	public void setTimestamp(String timestamp) {
//		this.timestamp = timestamp;
//	}
//
//	public Double getCurr_latitude() {
//		return curr_latitude;
//	}
//
//	public void setCurr_latitude(Double curr_latitude) {
//		this.curr_latitude = curr_latitude;
//	}
//
//	public Double getCurr_longitude() {
//		return curr_longitude;
//	}
//
//	public void setCurr_longitude(Double curr_longitude) {
//		this.curr_longitude = curr_longitude;
//	}
//
//	public Double getLast_latitude() {
//		return last_latitude;
//	}
//
//	public void setLast_latitude(Double last_latitude) {
//		this.last_latitude = last_latitude;
//	}
//
//	public Double getLast_longitude() {
//		return last_longitude;
//	}
//
//	public void setLast_longitude(Double last_longitude) {
//		this.last_longitude = last_longitude;
//	}
//}
