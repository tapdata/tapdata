package com.tapdata.entity;

import java.io.Serializable;

public class User implements Serializable {

	private static final long serialVersionUID = 3030275449023717172L;

	public static final int ADMIN_ROLE = 1;

	public static final int AGENT_ROLE = 0;

	private String id;

	private String password;

	private String email;

	private Integer role;

	private Integer account_status;

	private String accesscode;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public Integer getRole() {
		return role;
	}

	public void setRole(Integer role) {
		this.role = role;
	}

	public Integer getAccount_status() {
		return account_status;
	}

	public void setAccount_status(Integer account_status) {
		this.account_status = account_status;
	}

	public String getAccesscode() {
		return accesscode;
	}

	public void setAccesscode(String accesscode) {
		this.accesscode = accesscode;
	}
}
