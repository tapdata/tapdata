package io.tapdata.common;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.error.CoreException;
import io.tapdata.common.support.APIFactory;
import io.tapdata.common.support.APIInvoker;

import java.util.Map;
import java.util.Objects;

/**
 * @author aplomb
 */
@Implementation(APIFactory.class)
public class APIFactoryImpl implements APIFactory {

	@Override
	public APIInvoker loadAPI(String apiContent, String type, Map<String, Object> params) {
		if (Objects.isNull(type)) type = APIFactory.TYPE_POSTMAN;
		switch (type){
			case APIFactory.TYPE_POSTMAN: return PostManAPIInvoker.create().analysis(apiContent,params);
			//case APIFactory.TYPE_API_FOX: return PostManAPIInvoker.create().analysis(apiContent,params);
		}
		throw new CoreException(String.format("The current type is temporarily not supported. Not supported: %s .",type));
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
