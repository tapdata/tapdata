package io.tapdata.common.support;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.common.support.entitys.APIEntity;
import io.tapdata.common.support.entitys.APIResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * @author aplomb
 */
public interface APIInvoker {
	public APIResponse invoke(String uriOrName, Map<String, Object> params, String method, boolean invoker);

	public default APIResponse invoke(String uriOrName, Map<String, Object> params, String method){
		return this.invoke(uriOrName, params, method,false);
	}

	public default APIResponse invoke(String uriOrName, Map<String, Object> params){
		return this.invoke(uriOrName, params,"POST",false);
	}

	public default APIResponse invoke(String uriOrName, String method){
		return this.invoke(uriOrName, new HashMap<>(), method,false);
	}

	public default APIResponse invoke(String uriOrName){
		return this.invoke(uriOrName, new HashMap<>(), "POST",false);
	}


	void setAPIResponseInterceptor(APIResponseInterceptor interceptor);

	public List<APIEntity> tableApis();

	public List<String> tables();

	public List<APIEntity> tokenApis();

	public List<String> tokenApiNames();

	public Map<String,Object> variable();

	public void setConfig(Object configMap);

	public void setConnectorConfig(Object configMap);

	public void addConnectorConfig(Object setConnectorConfig);

	public void pageStage(TapConnectorContext connectorContext,
							   TapTable table,
							   Object offset,
							   int batchCount,
							   AtomicBoolean task,
							   BiConsumer<List<TapEvent>, Object> consumer);

	public void iterateAllData(String urlOrName, String method, Object offset, APIIterateInterceptor interceptor);
}
