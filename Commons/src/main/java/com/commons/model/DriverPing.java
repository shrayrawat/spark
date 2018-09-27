package com.commons.model;

import com.github.davidmoten.geo.LatLong;

public class DriverPing {
	private String driverId;
	private LatLong position;
	private Long timestamp;

	public DriverPing(String driverId, LatLong position, Long timestamp) {
		super();
		this.driverId = driverId;
		this.position = position;
		this.timestamp = timestamp;
	}

	public String getDriverId() {
		return driverId;
	}

	public LatLong getPosition() {
		return position;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	@Override
	public String toString() {
		return "DriverPing [driverId=" + driverId + ", position=" + position + ", timestamp=" + timestamp + "]";
	}

}
