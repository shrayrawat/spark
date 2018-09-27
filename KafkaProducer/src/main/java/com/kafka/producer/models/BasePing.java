package com.kafka.producer.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BasePing {
	@JsonProperty("timestamp")
	private String timestamp;
	@JsonProperty("curr_latitude")
	private Double curr_latitude;
	@JsonProperty("curr_longitude")
	private Double curr_longitude;

	public BasePing() {
	}

	public BasePing(String timestamp, Double curr_latitude, Double curr_longitude) {
		super();
		this.timestamp = timestamp;
		this.curr_latitude = curr_latitude;
		this.curr_longitude = curr_longitude;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public Double getCurr_latitude() {
		return curr_latitude;
	}

	public Double getCurr_longitude() {
		return curr_longitude;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
}
