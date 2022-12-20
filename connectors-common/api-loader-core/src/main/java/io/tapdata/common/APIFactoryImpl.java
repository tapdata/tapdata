package io.tapdata.common;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.utils.ClassFactory;
import io.tapdata.common.api.APIFactory;
import io.tapdata.common.api.APIInvoker;
import io.tapdata.common.support.PostManAPIInvoker;

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
