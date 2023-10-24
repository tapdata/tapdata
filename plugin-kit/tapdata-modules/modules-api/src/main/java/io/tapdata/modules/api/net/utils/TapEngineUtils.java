package io.tapdata.modules.api.net.utils;

public interface TapEngineUtils {
	String signUrl(String reqMethod, String url);
	String signUrl(String reqMethod, String url, String bodyStr);

	Integer getRealWsPort(Integer wsPort, String baseUrl);

	String getRealWsPath(String wsPath, String loginUrl);
}
