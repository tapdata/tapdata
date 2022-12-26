package io.tapdata.common;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.pdk.apis.javascript.APIFactory;
import io.tapdata.pdk.apis.javascript.APIInvoker;

import java.util.Map;

/**
 * @author aplomb
 */
@Implementation(APIFactory.class)
public class APIFactoryImpl implements APIFactory {
	@Override
	public APIInvoker loadAPI(String apiContent, String type, Map<String, Object> params) {
		//PostManAPIInvoker apiFactory = PostManAnalysis.create();//ClassFactory.create(PostManAPIInvoker.class);
		return PostManAPIInvoker.create().analysis(apiContent, params);
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
