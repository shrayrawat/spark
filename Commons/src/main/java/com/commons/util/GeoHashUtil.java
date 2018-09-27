package com.commons.util;

import com.github.davidmoten.geo.GeoHash;
import com.github.davidmoten.geo.LatLong;

public class GeoHashUtil {
	private static final int GEO_PRECISION = 6;

	public static String toGeohash(double lat, double lon) {
		return GeoHash.encodeHash(lat, lon, GEO_PRECISION);
	}

	public static String toGeohash(LatLong latlong) {
		return GeoHash.encodeHash(latlong, GEO_PRECISION);
	}

	public static LatLong toLatLong(String geohash) {
		return GeoHash.decodeHash(geohash);
	}
}
