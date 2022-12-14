package io.tapdata.api;

import io.tapdata.api.invoker.PostManAPIInvoker;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.utils.ClassFactory;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.pdk.apis.api.APIFactory;
import io.tapdata.pdk.apis.api.APIInvoker;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author aplomb
 */
@Implementation(APIFactory.class)
public class APIFactoryImpl implements APIFactory {
	@Override
	public APIInvoker loadAPI(String apiContent, String type, Map<String, Object> params) {
		PostManAPIInvoker apiFactory = ClassFactory.create(PostManAPIInvoker.class);
		apiFactory.analysis(type,apiContent,params);
		return apiFactory;
	}

	@Override
	public APIInvoker loadAPI(Map<String, Object> params) {
		return loadAPI(null,null,params);
	}

	@Override
	public APIInvoker loadAPI() {
		return loadAPI(null,null,null);
	}

}
