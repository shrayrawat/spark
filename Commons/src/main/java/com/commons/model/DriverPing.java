package com.commons.model;

import java.io.Serializable;

public class DriverPing implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String driverId;
	private LatAndLong position;
	private Long timestamp;

	public DriverPing(String driverId, LatAndLong position, Long timestamp) {
		super();
		this.driverId = driverId;
		this.position = position;
		this.timestamp = timestamp;
	}

	public String getDriverId() {
		return driverId;
	}

	public LatAndLong getPosition() {
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
