/**
 * @title: WebSocketInfo
 * @description:
 * @author lk
 * @date 2021/9/9
 */
package com.tapdata.tm.ws.dto;

import org.springframework.http.HttpHeaders;
import org.springframework.web.socket.WebSocketSession;

import java.util.List;

public class WebSocketInfo {

	private String uid;

	private String userId;

	private WebSocketSession session;

	private String language;

	public WebSocketInfo(String uid, String userId, WebSocketSession session) {
		this.uid = uid;
		this.userId = userId;
		this.session = session;

		HttpHeaders headers = session.getHandshakeHeaders();
		List<String> cookieList = headers.get("cookie");
		if (null!=cookieList) {
			String[] cookies = cookieList.get(0).replaceAll(" ", "").split(";");
			for (int index = 0; index < cookies.length; index++) {
				if (cookies[index].startsWith("lang=")){
					this.setLanguage(cookies[index].substring(cookies[index].indexOf("=")+1));
					break;
				}
			}
		}
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
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
