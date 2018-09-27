package com.commons.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TrafficData {
	@JsonProperty
	private String geohash;
	@JsonProperty
	private double timestamp;
	@JsonProperty("avgspeed")
	private double avgSpeed;
	@JsonProperty("normalized")
	private double normalizedSpeed;
	private String weather;

	public TrafficData() {
	}

	public TrafficData(String geohash, double timestamp, double avgSpeed, double normalizedSpeed, String weather) {
		super();
		this.geohash = geohash;
		this.timestamp = timestamp;
		this.avgSpeed = avgSpeed;
		this.normalizedSpeed = normalizedSpeed;
		this.weather = weather;
	}

	public String getGeohash() {
		return geohash;
	}

	public void setGeohash(String geohash) {
		this.geohash = geohash;
	}

	public double getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(double timestamp) {
		this.timestamp = timestamp;
	}

	public double getAvgSpeed() {
		return avgSpeed;
	}

	public void setAvgSpeed(double avgSpeed) {
		this.avgSpeed = avgSpeed;
	}

	public double getNormalizedSpeed() {
		return normalizedSpeed;
	}

	public void setNormalizedSpeed(double normalizedSpeed) {
		this.normalizedSpeed = normalizedSpeed;
	}

	public String getWeather() {
		return weather;
	}

	public void setWeather(String weather) {
		this.weather = weather;
	}

}
