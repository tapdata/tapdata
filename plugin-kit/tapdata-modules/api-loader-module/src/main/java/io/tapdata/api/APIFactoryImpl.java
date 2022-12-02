package io.tapdata.api;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.pdk.apis.api.APIFactory;
import io.tapdata.pdk.apis.api.APIInvoker;

import java.util.Map;

/**
 * @author aplomb
 */
@Implementation(APIFactory.class)
public class APIFactoryImpl implements APIFactory {
	@Override
	public APIInvoker loadAPI(String apiContent, String type, Map<String, Object> params) {
		return null;
	}
}
