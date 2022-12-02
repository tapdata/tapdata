package io.tapdata.api.postman;

import io.tapdata.pdk.apis.api.APIInvoker;

import java.util.Map;

/**
 * @author aplomb
 */
public class PostManAPIInvoker implements APIInvoker {
	@Override
	public Map<String, Object> invoke(String uri, String method, Map<String, Object> params) {
		loadApiMap();

		return null;
	}
	private Map<String,Object> loadApiMap(){

		return null;
	}
}
