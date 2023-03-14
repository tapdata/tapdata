package com.tapdata.mongo;

import com.tapdata.tm.sdk.util.CloudSignUtil;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.modules.api.net.utils.TapEngineUtils;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.net.MalformedURLException;
import java.net.URL;

@Implementation(TapEngineUtils.class)
public class TapEngineUtilsImpl implements TapEngineUtils {
	@Override
	public String signUrl(String reqMethod, String url) {
		if(CloudSignUtil.isNeedSign()) {
			return CloudSignUtil.getQueryStr(reqMethod, url);
		}
		return url;
	}

	@Override
	public String signUrl(String reqMethod, String url, String bodyStr) {
		if(CloudSignUtil.isNeedSign())
			return CloudSignUtil.getQueryStr(reqMethod, url, bodyStr);
		return url;
	}

	@Override
	public Integer getRealWsPort(Integer wsPort, String baseUrl) {
		if(CloudSignUtil.isNeedSign()) {
			URL url;
			try {
				url = new URL(baseUrl);
			} catch (MalformedURLException e) {
				throw new RuntimeException(e);
			}
			int port = url.getPort();
			if(port > 0) {
				return port;
			} else {
				if(url.getProtocol().equals("http")) {
					return 80;
				} else if(url.getProtocol().equals("https")) {
					return 443;
				}
			}
		} else {
			String port = CommonUtils.getProperty("proxy_port");
			if(port != null) {
				try {
					return Integer.parseInt(port);
				} catch (Throwable ignored) {}
			}
		}
		return wsPort;
	}

	@Override
	public String getRealWsPath(String wsPath, String loginUrl) {
		int pos = wsPath.indexOf("engine/");
		if(pos < 0)
			throw new IllegalArgumentException("wsPath doesn't contain \"engine\", wsPath " + wsPath);
		String suffix = wsPath.substring(pos);
		try {
			URL url = new URL(loginUrl);
			String path = url.getPath();
			int proxyPos = path.indexOf("api/proxy");
			if(proxyPos < 0) {
				throw new IllegalArgumentException("loginUrl doesn't contain \"api/proxy\", loginUrl " + loginUrl);
			}
			String prefix = path.substring(0, proxyPos);
			if(prefix.startsWith("/")) {
				prefix = prefix.substring(1);
			}
			return prefix + suffix;
		} catch (MalformedURLException e) {
			throw new RuntimeException(e);
		}
	}

}
