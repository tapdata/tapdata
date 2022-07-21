package com.tapdata.entity.Message;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2020-11-03 12:26
 **/
public class Message implements Serializable {

	private static final long serialVersionUID = 7941472723136765267L;

	private String level;

	private String system;

	private String msg;

	private String title;

	private String userId;

	private String email;

	private String serverName;

	private Date last_updated;

	private Date createTime;

	private boolean read;

	private String sourceId;

	public Message(String level, String system, String msg, String title, String userId, String email, String serverName, String sourceId) {
		if (StringUtils.isAnyBlank(level, system, msg, title, userId, email, serverName, sourceId)) {
			throw new IllegalArgumentException("Input params: level, system, msg, title, userId, email, serverName, sourceId cannot be empty");
		}
		this.level = level;
		this.system = system;
		this.msg = msg;
		this.title = title;
		this.userId = userId;
		this.email = email;
		this.serverName = serverName;
		this.sourceId = sourceId;
	}

	public String getLevel() {
		return level;
	}

	public void setLevel(String level) {
		this.level = level;
	}

	public String getSystem() {
		return system;
	}

	public void setSystem(String system) {
		this.system = system;
	}

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public Date getLast_updated() {
		return last_updated;
	}

	public void setLast_updated(Date last_updated) {
		this.last_updated = last_updated;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public boolean isRead() {
		return read;
	}

	public void setRead(boolean read) {
		this.read = read;
	}

	public String getServerName() {
		return serverName;
	}

	public void setServerName(String serverName) {
		this.serverName = serverName;
	}

	public String getSourceId() {
		return sourceId;
	}

	public void setSourceId(String sourceId) {
		this.sourceId = sourceId;
	}

	@Override
	public String toString() {
		return "Message{" +
				"level='" + level + '\'' +
				", system='" + system + '\'' +
				", msg='" + msg + '\'' +
				", title='" + title + '\'' +
				", userId='" + userId + '\'' +
				", email='" + email + '\'' +
				", serverName='" + serverName + '\'' +
				", last_updated=" + last_updated +
				", createTime=" + createTime +
				", read=" + read +
				", sourceId='" + sourceId + '\'' +
				'}';
	}

	public enum Level {
		INFO("info"),
		WARN("warn"),
		ERROR("error"),
		;

		private String level;

		Level(String level) {
			this.level = level;
		}

		public String getLevel() {
			return level;
		}
	}

	public enum System {
		JOBDDL("JobDDL", "com.tapdata.entity.Message.JobDdlMessage"),
		;

		private String system;
		private String classPath;

		System(String system, String classPath) {
			this.system = system;
			this.classPath = classPath;
		}

		public String getSystem() {
			return system;
		}

		public String getClassPath() {
			return classPath;
		}

		static Map<String, System> map = new HashMap<>();

		static {
			Arrays.stream(System.values()).forEach(s -> {
				map.put(s.getSystem(), s);
			});
		}

		public static System fromSystem(String system) {
			return map.getOrDefault(system, null);
		}
	}
}
