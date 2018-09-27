//package com.util;
//
//import java.io.BufferedReader;
//import java.io.BufferedWriter;
//import java.io.FileReader;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Set;
//
//import com.fasterxml.jackson.annotation.JsonIgnore;
//import com.fasterxml.jackson.annotation.JsonProperty;
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.github.davidmoten.geo.GeoHash;
//
//public class DataUtil {
//	private static final String SEPARATOR = ",";
//	private static final String ID_PREFIX = "CT";
//	private static final Long INITIAL_ID = 11000l;
//	private static final String OUTPUT_FILE = "/Users/srawat1/Desktop/traffic/demand_data.csv";
//	private static final String INPUT_FILE = "/Users/srawat1/Desktop/traffic/green_tripdata_2018-01.csv";
//	public static final String ID_KEY = "customer_id";
//	private static Set<String> geoSet = new HashSet();
//
//	public static void main(String[] args) throws JsonProcessingException {
//		List<String> rows = new Reader().read(INPUT_FILE);
//		List<String> transformed = transform(rows);
//		System.out.println("transformed = " + transformed.size());
//		System.out.println("geoSet = " + geoSet.size());
//		new Writer().write(OUTPUT_FILE, transformed);
//	}
//
//	public static List<String> transform(List<String> rows) throws JsonProcessingException {
//
//		ObjectMapper mapper = new ObjectMapper();
//		Long Id = new Long(INITIAL_ID);
//
//		List<String> transformed = new ArrayList();
//		BaseRecord record;
//		int i = 0;
//		for (String row : rows) {
//			if (row.trim().isEmpty())
//				continue;
//			String[] tokens = row.split(SEPARATOR);
//			record = new BaseRecord(ID_PREFIX + (++Id), tokens[1], Double.parseDouble(tokens[6]),
//					Double.parseDouble(tokens[5]), Double.parseDouble(tokens[8]), Double.parseDouble(tokens[7]));
//
//			try {
//				geoSet.add(GeoHash.encodeHash(record.getCurr_latitude(), record.getCurr_longitude(), 6));
//				String json = mapper.writeValueAsString(record);
//				transformed.add(json);
//			} catch (Exception e) {
//				System.out.println("Invalid Lat-long : " + row);
//			}
//
//			if (i > 100)
//				break;
//		}
//		return transformed;
//	}
//}
//
//class Reader {
//
//	public List<String> read(String file) {
//
//		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
//			String sCurrentLine;
//			List<String> rows = new ArrayList();
//			int i = 1;
//			while ((sCurrentLine = br.readLine()) != null) {
//				if (i++ == 1) {
//					continue;
//				}
//
//				if (i == 500) {
//					break;
//				}
//
//				rows.add(sCurrentLine);
//			}
//			System.out.println("Rows read : " + rows.size());
//			return rows;
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		return null;
//	}
//}
//
//class Writer {
//
//	public void write(String file, List<String> rows) {
//		System.out.println("Writing to file now...");
//		try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
//			for (String row : rows) {
//				bw.write(row);
//				bw.newLine();
//			}
//			System.out.println("Rows Written : " + rows.size());
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
//
//}
//
//class BaseRecord {
//
//	@JsonProperty(DataUtil.ID_KEY)
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
//	public BaseRecord(String id, String timestamp, Double curr_latitude, Double curr_longitude, Double last_latitude,
//			Double last_longitude) {
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
