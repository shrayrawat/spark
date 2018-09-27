package com.exercise.models;

import java.io.Serializable;

public class GenericTopicData implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String geoHash;
	String timestamp;
	int requestSize;
	String requestName;

	@Override
	public String toString() {
		return "{\"geoHash\":\"" + geoHash + "\",\"timestamp\":\"" + timestamp + "\",\"requestSize\":" + requestSize
				+ ",\"requestName\":\"" + requestName + "\"}";

	}

	public GenericTopicData(String geoHash, String timestamp, int requestSize, String requestName) {
		super();
		this.geoHash = geoHash;
		this.timestamp = timestamp;
		this.requestSize = requestSize;
		this.requestName = requestName;
	}

	public String getGeoHash() {
		return geoHash;
	}

	public void setGeoHash(String geoHash) {
		this.geoHash = geoHash;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public int getRequestSize() {
		return requestSize;
	}

	public void setRequestSize(int requestSize) {
		this.requestSize = requestSize;
	}

	public String getRequestName() {
		return requestName;
	}

	public void setRequestName(String requestName) {
		this.requestName = requestName;
	}

}