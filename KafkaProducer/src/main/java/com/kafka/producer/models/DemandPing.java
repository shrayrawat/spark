package com.kafka.producer.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DemandPing extends BasePing {
	@JsonProperty("customer_id")
	private String id;

	public DemandPing() {
	}

	public DemandPing(String id, String timestamp, Double curr_latitude, Double curr_longitude) {
		super(timestamp, curr_latitude, curr_longitude);
		this.id = id;
	}

	public String getId() {
		return id;
	}
}
