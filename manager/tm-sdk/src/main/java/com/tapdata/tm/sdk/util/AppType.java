package com.tapdata.tm.sdk.util;


/**
 * @author samuel
 * @Description
 * @create 2021-05-29 04:30
 **/
public enum AppType {
	DRS, DFS, DAAS,
	;

	public boolean isDrs() {
		return DRS == this;
	}

	public boolean isDfs() {
		return DFS == this;
	}

	public boolean isDaas() {
		return DAAS == this;
	}

	public boolean isCloud() {
		return DRS == this || DFS == this;
	}

	public static AppType init() {
		String appTypeStr = System.getProperty("app_type");
		if (appTypeStr == null || "".equals(appTypeStr)) {
			throw new RuntimeException("app_type is blank");
		}
		AppType appType;
		try {
			appType = AppType.valueOf(appTypeStr.toUpperCase());
		} catch (IllegalArgumentException e) {
			throw new RuntimeException("nonsupport app_type: " + appTypeStr);
		}
		return appType;
	}
}
