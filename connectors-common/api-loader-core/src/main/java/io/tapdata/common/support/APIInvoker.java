package io.tapdata.common.support;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.common.support.entitys.APIEntity;
import io.tapdata.common.support.entitys.APIResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * @author aplomb
 */
public interface APIInvoker {
	public APIResponse invoke(String uriOrName, String method, Map<String, Object> params, boolean invoker);

	public default APIResponse invoke(String uriOrName, String method, Map<String, Object> params){
		return this.invoke(uriOrName, method, params,false);
	}

	public default APIResponse invoke(String uriOrName, String method){
		return this.invoke(uriOrName, method, new HashMap<>(),false);
	}

	public default APIResponse invoke(String uriOrName){
		return this.invoke(uriOrName, "POST", new HashMap<>(),false);
	}


	void setAPIResponseInterceptor(APIResponseInterceptor interceptor);

	public List<APIEntity> tableApis();

	public List<String> tables();

	public List<APIEntity> tokenApis();

	public List<String> tokenApiNames();

	public Map<String,Object> variable();

	public void pageStage(TapConnectorContext connectorContext,
							   TapTable table,
							   Object offset,
							   int batchCount,
							   AtomicBoolean task,
							   BiConsumer<List<TapEvent>, Object> consumer);

	public void iterateAllData(String urlOrName, String method, Object offset, APIIterateInterceptor interceptor);
}
