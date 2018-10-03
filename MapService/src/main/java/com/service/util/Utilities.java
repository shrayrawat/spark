package com.service.util;

import java.io.IOException;
import java.util.Map;

import com.commons.model.OutputRecordData;
import com.commons.model.TrafficData;
import com.commons.util.GeoHashUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.davidmoten.geo.LatLong;
import com.google.common.collect.ImmutableMap;
import com.models.LocationHolder;

public class Utilities {
	private static ObjectMapper mapper = new ObjectMapper();

	public static String toHTML(Map<String, String> pairs) {
		StringBuilder sb = new StringBuilder();
		for (String key : pairs.keySet()) {
			sb.append("<b>").append(key).append(":").append("</b> &emsp;").append(pairs.get(key)).append("<br>");
		}
		return sb.toString();
	}

	public static LocationHolder toTrafficGeoPoint(Map<String, String> records) throws IOException {
		for (Map.Entry<String, String> entry : records.entrySet()) {
			TrafficData tr = mapper.readValue(entry.getValue(), TrafficData.class);
			LatLong ll = GeoHashUtil.toLatLong(tr.getGeohash());

			Map<String, String> pairs = ImmutableMap.of("Avg. Speed", String.format("%.2f", tr.getAvgSpeed()),
					"Normalized", String.format("%.2f", tr.getNormalizedSpeed()), "Weather", tr.getWeather());
			return new LocationHolder(ll.getLat(), ll.getLon(), toHTML(pairs));
		}
		return null;
	}

	public static LocationHolder toSupplyDemandGeoPoint(Map<String, String> records) throws IOException {
		for (Map.Entry<String, String> entry : records.entrySet()) {
			OutputRecordData or = mapper.readValue(entry.getValue(), OutputRecordData.class);
			LatLong ll = GeoHashUtil.toLatLong(entry.getKey());

			Map<String, String> pairs = ImmutableMap.of("S/D Ratio", String.format("%.2f", or.getRatio()), "Weather",
					or.getWeather());
			return new LocationHolder(ll.getLat(), ll.getLon(), toHTML(pairs));
		}
		return null;
	}
}
