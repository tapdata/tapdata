package com.tapdata.constant;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConfigurationCenter implements Serializable {

	public static final String TOKEN = "token";
	public static final String USER_ID = "userId";
	public static final String USER_INFO = "userInfo";
	public static final String BASR_URLS = "baseURLs";
	public static final String RETRY_TIME = "retryTime";
	public static final String AGENT_ID = "agentId";
	public static final String LOGIN_INFO = "loginInfo";
	public static final String ACCESS_CODE = "accessCode";
	public static final String IS_CLOUD = "is_cloud";
	public static final String JOB_TAGS = "jobTags";
	public static final String REGION = "region";
	public static final String ZONE = "zone";
	public static final String APPTYPE = "appType";
	public static final String WORK_DIR = "workDir";
	private static final long serialVersionUID = -204086852762105757L;
	public static String processId;

	private Map<String, Object> config = new ConcurrentHashMap<>();

	public ConfigurationCenter() {
	}

	public void putConfig(String key, Object value) {
		config.put(key, value);
	}

	public Object getConfig(String key) {
		return config.get(key);
	}

	public <E> E getConfig(String key, Class<E> clazz) {
		Object obj = getConfig(key);
		if (clazz.isInstance(obj)) {
			return (E) obj;
		}
		return null;
	}

	public Object clone() {
		ConfigurationCenter configurationCenter = new ConfigurationCenter();
		configurationCenter.config.putAll(this.config);
		return configurationCenter;
	}

	public Object getConfig(String key, Object defaultReturn) {
		return config.getOrDefault(key, defaultReturn);
	}
}
