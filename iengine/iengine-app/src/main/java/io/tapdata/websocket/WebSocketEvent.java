package io.tapdata.websocket;

import java.io.Serializable;

/**
 * @author lg
 * Create by lg on 6/4/20 8:45 PM
 */
public class WebSocketEvent<T> implements Serializable {

	private String type;

	private String receiver;

	private String sender;

	private T data;

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getReceiver() {
		return receiver;
	}

	public void setReceiver(String receiver) {
		this.receiver = receiver;
	}

	public String getSender() {
		return sender;
	}

	public void setSender(String sender) {
		this.sender = sender;
	}

	public T getData() {
		return data;
	}

	public void setData(T data) {
		this.data = data;
	}

	@Override
	public String toString() {
		return "WebSocketEvent{" +
				"type='" + type + '\'' +
				", receiver='" + receiver + '\'' +
				", sender='" + sender + '\'' +
				", data=" + data +
				'}';
	}
}
