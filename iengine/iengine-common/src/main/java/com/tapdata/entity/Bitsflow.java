package com.tapdata.entity;

public class Bitsflow {

	private String topic;

	private String data;

	public Bitsflow(String topic, String data) {
		this.topic = topic;
		this.data = data;
	}

	public String getTopic() {
		return topic;
	}

	public String getData() {
		return data;
	}

}
