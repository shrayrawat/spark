package com.exercise.spark.traffic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.commons.model.DriverPing;
import com.commons.model.LatAndLong;
import com.commons.util.GeoHashUtil;
import com.github.davidmoten.geo.LatLong;

public class TrafficUtility {
	private static final double MAX_SPEED = 16;

	public static Map<String, List<Double>> getSpeedValues(List<DriverPing> beacons) {

		sortByTimestamp(beacons);

		Map<String, List<Double>> geohashSpeedMap = new HashMap();
		Iterator<DriverPing> iterator = beacons.iterator();
		DriverPing lastBeacon = null, currentBeacon;

		if (iterator.hasNext())
			lastBeacon = iterator.next();

		while (iterator.hasNext()) {
			currentBeacon = iterator.next();
			if (sameGeohash(currentBeacon, lastBeacon)) {
				double dist = distance(currentBeacon.getPosition(), lastBeacon.getPosition());
				double time = timeDifference(currentBeacon, lastBeacon);
				if (time == 0)
					continue;
				double avgSpeed = toMetersPerSecond(dist / time);
				String geohash = GeoHashUtil.toGeohash(
						new LatLong(currentBeacon.getPosition().getLat(), currentBeacon.getPosition().getLon()));

				if (!geohashSpeedMap.containsKey(geohash)) {
					geohashSpeedMap.put(geohash, new ArrayList());
				}
				geohashSpeedMap.get(geohash).add(avgSpeed);
				lastBeacon = currentBeacon;
			}
		}
		return geohashSpeedMap;
	}

	public static void sortByTimestamp(List<DriverPing> beacons) {
		Comparator<DriverPing> COMPARATOR = (o1, o2) -> (int) (o1.getTimestamp() - o2.getTimestamp());
		Collections.sort(beacons, COMPARATOR);
	}

	private static boolean sameGeohash(DriverPing beacon1, DriverPing beacon2) {
		if (beacon1 == null || beacon2 == null)
			return false;
		return GeoHashUtil.toGeohash(new LatLong(beacon1.getPosition().getLat(), beacon1.getPosition().getLon()))
				.equals(GeoHashUtil
						.toGeohash(new LatLong(beacon2.getPosition().getLat(), beacon2.getPosition().getLon())));
	}

	public static Map<String, List<Double>> merge(Map<String, List<Double>> map1, Map<String, List<Double>> map2) {
		for (String key : map1.keySet()) {
			if (map2.containsKey(key)) {
				List<Double> value1 = map1.get(key);
				List<Double> value2 = map2.get(key);
				value1.addAll(value2);
				map2.remove(key);
			}
		}
		map1.putAll(map2);
		return map1;
	}

	public static double computeMean(List<Double> values) {
		double sum = 0;
		for (Double d : values) {
			sum += d;
		}
		return sum / values.size();
	}

	public static double normalize(double value) {
		double normalized = value / MAX_SPEED;
		return normalized > 1 ? 1 : normalized;
	}

	private static double toMetersPerSecond(double value) {
		return value * Math.pow(10, 6);
	}

	private static double timeDifference(DriverPing curr, DriverPing last) {
		return curr.getTimestamp() - last.getTimestamp();
	}

	public static final double distance(LatAndLong latLong1, LatAndLong latLong2) {
		return distance(latLong1.getLat(), latLong1.getLon(), latLong2.getLat(), latLong2.getLon());
	}

	public static final double distance(double lat1, double lon1, double lat2, double lon2) {
		double theta = lon1 - lon2;
		double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2))
				+ Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
		dist = Math.acos(dist);
		dist = rad2deg(dist);
		dist = dist * 60 * 1.1515;
		dist = dist * 1.609344; // Convert to KM
		return (dist);
	}

	private static final double deg2rad(double deg) {
		return (deg * Math.PI / 180.0);
	}

	private static final double rad2deg(double rad) {
		return (rad * 180 / Math.PI);
	}
}
