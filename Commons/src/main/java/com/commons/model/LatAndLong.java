package com.commons.model;

import java.io.Serializable;

public class LatAndLong implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final double lat;
	private final double lon;

	/**
	 * Constructor.
	 * 
	 * @param lat
	 * @param lon
	 */
	public LatAndLong(double lat, double lon) {
		super();
		this.lat = lat;
		this.lon = lon;
	}

	/**
	 * Returns the latitude in decimal degrees.
	 * 
	 * @return
	 */
	public double getLat() {
		return lat;
	}

	/**
	 * Returns the longitude in decimal degrees.
	 * 
	 * @return
	 */
	public double getLon() {
		return lon;
	}

	public LatAndLong add(double deltaLat, double deltaLon) {
		return new LatAndLong(lat + deltaLat, lon + deltaLon);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("LatAndLong [lat=");
		builder.append(lat);
		builder.append(", lon=");
		builder.append(lon);
		builder.append("]");
		return builder.toString();
	}

}
