package com.tapdata.mongo;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.modules.api.net.utils.TapEngineUtils;

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
}
