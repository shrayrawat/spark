package com.commons.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BatchRecord {

	@JsonProperty
	private String geohash;
	@JsonProperty
	private String timestamp;
	@JsonProperty
	private long supply;
	@JsonProperty
	private long demand;
	@JsonProperty
	private String weather;

	public BatchRecord(String geohash, String timestamp, long supply, long demand, String weather) {
		this.geohash = geohash;
		this.timestamp = timestamp;
		this.supply = supply;
		this.demand = demand;
		this.weather = weather;
	}

	public String getGeohash() {
		return geohash;
	}

	public void setGeohash(String geohash) {
		this.geohash = geohash;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public long getSupply() {
		return supply;
	}

	public void setSupply(long supply) {
		this.supply = supply;
	}

	public long getDemand() {
		return demand;
	}

	public void setDemand(long demand) {
		this.demand = demand;
	}

	public String getWeather() {
		return weather;
	}

	public void setWeather(String weather) {
		this.weather = weather;
	}
}
