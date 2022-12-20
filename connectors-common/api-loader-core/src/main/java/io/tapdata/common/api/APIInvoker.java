package io.tapdata.common.api;

import java.util.Map;

/**
 * @author aplomb
 */
public interface APIInvoker {
	APIResponse invoke(String uriOrName, String method, Map<String, Object> params, boolean invoker);

	void setAPIResponseInterceptor(APIResponseInterceptor interceptor);

}
