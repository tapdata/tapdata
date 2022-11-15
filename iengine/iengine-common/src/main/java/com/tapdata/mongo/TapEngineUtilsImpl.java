package com.tapdata.mongo;

import com.tapdata.tm.sdk.util.CloudSignUtil;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.modules.api.net.utils.TapEngineUtils;

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
		}
		return wsPort;
	}

	@Override
	public String getRealWsPath(String wsPath) {
		if(CloudSignUtil.isNeedSign()) {
			return "console/tm/" + wsPath;
		}
		return wsPath;
	}
}
