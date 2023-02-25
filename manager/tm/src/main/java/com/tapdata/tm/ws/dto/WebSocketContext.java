/**
 * @title: WebSocketContext
 * @description:
 * @author lk
 * @date 2021/9/9
 */
package com.tapdata.tm.ws.dto;

public class WebSocketContext {

	private String sessionId;

	private String sender;

	private String userId;

	private MessageInfo messageInfo;

	public WebSocketContext(String sender, String userId, MessageInfo messageInfo) {
		this.sender = sender;
		this.userId = userId;
		this.messageInfo = messageInfo;
	}

	public WebSocketContext(String sessionId, String sender, String userId, MessageInfo messageInfo) {
		this.sessionId = sessionId;
		this.sender = sender;
		this.userId = userId;
		this.messageInfo = messageInfo;
	}

	public String getSender() {
		return sender;
	}

	public String getUserId() {
		return userId;
	}

	public MessageInfo getMessageInfo() {
		return messageInfo;
	}

	public void setSender(String sender) {
		this.sender = sender;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public void setMessageInfo(MessageInfo messageInfo) {
		this.messageInfo = messageInfo;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	@Override
	public String toString() {
		return "WebSocketContext{" +
				"sessionId='" + sessionId + '\'' +
				", sender='" + sender + '\'' +
				", userId='" + userId + '\'' +
				'}';
	}
}
