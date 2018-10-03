package com.webservice;

import org.jetbrains.annotations.NotNull;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.dropwizard.Configuration;

public class ServiceConfig extends Configuration {

	@NotNull
	@JsonProperty
	private String kafkaBroker;

	public String getKafkaBroker() {
		return kafkaBroker;
	}

	public void setKafkaBroker(String kafkaBroker) {
		this.kafkaBroker = kafkaBroker;
	}
}