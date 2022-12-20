package io.tapdata.common.support;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.common.api.APIFactory;
import io.tapdata.common.api.APIInvoker;
import io.tapdata.common.support.postman.PostManAnalysis;

import java.util.Map;

/**
 * @author aplomb
 */
@Implementation(APIFactory.class)
public class APIFactoryImpl implements APIFactory {
	@Override
	public APIInvoker loadAPI(String apiContent, String type, Map<String, Object> params) {
		//PostManAPIInvoker apiFactory = PostManAnalysis.create();//ClassFactory.create(PostManAPIInvoker.class);
		APIInvoker analysis = PostManAnalysis.create().analysis(apiContent, type, params);
		return analysis;
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
