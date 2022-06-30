package com.tapdata.entity;

import java.io.Serializable;

public class LoginResp implements Serializable {

	private String id;

	private String created;

	private String userId;

	private Long ttl;

	private long expiredTimestamp;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getCreated() {
		return created;
	}

	public void setCreated(String created) {
		this.created = created;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public Long getTtl() {
		return ttl;
	}

	public void setTtl(Long ttl) {
		this.ttl = ttl;
	}

	public long getExpiredTimestamp() {
		return expiredTimestamp;
	}

	public void setExpiredTimestamp(long expiredTimestamp) {
		this.expiredTimestamp = expiredTimestamp;
	}
}
