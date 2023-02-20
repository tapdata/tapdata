/**
 * @title: WebSocketInfo
 * @description:
 * @author lk
 * @date 2021/9/9
 */
package com.tapdata.tm.ws.dto;

import lombok.Getter;
import lombok.Setter;
import org.springframework.web.socket.WebSocketSession;

public class WebSocketInfo {
	/**
	 * web socket session id
	 */
	@Getter
	@Setter
	private String id;

	/**
	 * Flow engine process_id
	 */
	@Getter
	@Setter
	private String agentId;

	@Getter
	@Setter
	private String singletonTag;

	/**
	 * 用户ID
	 */
	@Getter
	@Setter
	private String userId;
	@Getter
	@Setter
	private WebSocketSession session;
	@Getter
	@Setter
	private String ip;
	/**
	 * last pong frame response timestamp
	 */
	@Getter
	@Setter
	private long lastKeepAliveTimestamp;



	public WebSocketInfo(String id, String agentId, String singletonTag, String userId, WebSocketSession session, String ip) {
		this.id = id;
		this.agentId = agentId;
		this.singletonTag = singletonTag;
		this.userId = userId;
		this.session = session;
		this.ip = ip;
	}

	@Override
	public String toString() {
		return "WebSocketInfo{" +
				"id='" + id + '\'' +
				", agentId='" + agentId + '\'' +
				", singletonTag='" + singletonTag + '\'' +
				", userId='" + userId + '\'' +
				", ip='" + ip + '\'' +
				'}';
	}
}
