/**
 * @title: WebSocketInfo
 * @description:
 * @author lk
 * @date 2021/9/9
 */
package com.tapdata.tm.ws.dto;

import org.springframework.web.socket.WebSocketSession;

public class WebSocketInfo {

	private String uid;

	private String userId;

	private WebSocketSession session;

	public WebSocketInfo(String uid, String userId, WebSocketSession session) {
		this.uid = uid;
		this.userId = userId;
		this.session = session;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getUserId() {
		return userId;
	}

	public WebSocketSession getSession() {
		return session;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public void setSession(WebSocketSession session) {
		this.session = session;
	}
}
