package io.tapdata.api;

import io.tapdata.api.postman.PostManAPIInvoker;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.utils.InstanceFactory;
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
		PostManAPIInvoker apiFactory = InstanceFactory.instance(PostManAPIInvoker.class);
		Map<String, Object> invoke = apiFactory.invoke(apiContent, type, params);
		return apiFactory;
	}
}
