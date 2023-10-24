package io.tapdata.pdk.core.api.impl;

import java.io.Serializable;

public class UserSerializable implements Serializable {
	protected String name;
	protected Long createTime;
	protected Long updateTime;
	protected String description;
	protected Integer gender;
	protected String email;

	protected UserSerializable user;

	public UserSerializable getUser() {
		return user;
	}

	public void setUser(UserSerializable user) {
		this.user = user;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Long getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Long createTime) {
		this.createTime = createTime;
	}

	public Long getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Long updateTime) {
		this.updateTime = updateTime;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Integer getGender() {
		return gender;
	}

	public void setGender(Integer gender) {
		this.gender = gender;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}
}
