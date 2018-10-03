package com.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LocationHolder {

	@JsonProperty("latitude")
	private Double latitude;

	@JsonProperty("longitude")
	private Double longitude;

	@JsonProperty("description")
	private String description;

	public LocationHolder(Double latitude, Double longitude, String description) {
		super();
		this.latitude = latitude;
		this.longitude = longitude;
		this.description = description;
	}

	public Double getLatitude() {
		return latitude;
	}

	public void setLatitude(Double latitude) {
		this.latitude = latitude;
	}

	public Double getLongitude() {
		return longitude;
	}

	public void setLongitude(Double longitude) {
		this.longitude = longitude;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	@Override
	public String toString() {
		return "LocationHolder [latitude=" + latitude + ", longitude=" + longitude + ", description=" + description
				+ "]";
	}

}
