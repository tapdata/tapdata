/**
 * @title: LogsCache
 * @description:
 * @author lk
 * @date 2021/9/14
 */
package com.tapdata.tm.ws.dto;

import java.util.concurrent.LinkedBlockingQueue;
import org.bson.Document;

public class LogsCache {

	private String sessionId;

	private String receiver;

	private LinkedBlockingQueue<Document> caches;

	private Boolean enabled;

	private Long lastTime;

	public LogsCache(String sessionId, String receiver) {
		this.sessionId = sessionId;
		this.receiver = receiver;
		this.enabled = true;
		this.caches = new LinkedBlockingQueue<>();
	}

	public String getReceiver() {
		return receiver;
	}

	public LinkedBlockingQueue<Document> getCaches() {
		return caches;
	}

	public Boolean getEnabled() {
		return enabled != null && enabled;
	}

	public void setReceiver(String receiver) {
		this.receiver = receiver;
	}

	public void setCaches(LinkedBlockingQueue<Document> caches) {
		this.caches = caches;
	}

	public void setEnabled(Boolean enabled) {
		this.enabled = enabled;
	}

	public Long getLastTime() {
		return lastTime;
	}

	public void setLastTime(Long lastTime) {
		this.lastTime = lastTime;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}
}
