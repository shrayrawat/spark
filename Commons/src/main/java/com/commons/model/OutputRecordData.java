package com.commons.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class OutputRecordData {

	@JsonProperty
	private long supply;
	@JsonProperty
	private long demand;
	@JsonProperty
	private double ratio;
	@JsonProperty
	private String weather;

	public OutputRecordData() {
	}

	public OutputRecordData(long supply, long demand, double ratio, String weather) {
		super();
		this.supply = supply;
		this.demand = demand;
		this.ratio = ratio;
		this.weather = weather;
	}

	public long getSupply() {
		return supply;
	}

	public long getDemand() {
		return demand;
	}

	public double getRatio() {
		return ratio;
	}

	public String getWeather() {
		return weather;
	}

	@Override
	public String toString() {
		return "OutputRecordData [supply=" + supply + ", demand=" + demand + ", ratio=" + ratio + ", weather=" + weather
				+ "]";
	}

}
