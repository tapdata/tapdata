package io.tapdata.common.api;

import io.tapdata.common.support.postman.entity.ApiMap;

import java.util.List;
import java.util.Map;

/**
 * @author aplomb
 */
public interface APIInvoker {
	APIResponse invoke(String uriOrName, String method, Map<String, Object> params, boolean invoker);

	void setAPIResponseInterceptor(APIResponseInterceptor interceptor);

	public List<ApiMap.ApiEntity> tableApis();

	public List<String> tables();

	public Map<String,Object> variable();
}
