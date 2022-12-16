package io.tapdata.quickapi;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.utils.ClassFactory;
import io.tapdata.quickapi.api.APIFactory;
import io.tapdata.quickapi.api.APIInvoker;
import io.tapdata.quickapi.support.PostManAPIInvoker;

import java.util.Map;

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
