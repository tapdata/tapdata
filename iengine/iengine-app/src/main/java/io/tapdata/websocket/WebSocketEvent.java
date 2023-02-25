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

	private String code;

	private String message;

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

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	@Override
	public String toString() {
		return "WebSocketEvent{" +
				"type='" + type + '\'' +
				", receiver='" + receiver + '\'' +
				", sender='" + sender + '\'' +
				", data=" + data +
				", code='" + code + '\'' +
				'}';
	}
}
