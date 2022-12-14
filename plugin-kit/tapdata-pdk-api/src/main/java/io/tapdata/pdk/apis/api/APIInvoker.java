package io.tapdata.pdk.apis.api;

import java.util.List;
import java.util.Map;

/**
 * @author aplomb
 */
public interface APIInvoker {
	APIResponse invoke(String uriOrName, String method, Map<String, Object> params);

	void setAPIResponseInterceptor(APIResponseInterceptor interceptor);

}
