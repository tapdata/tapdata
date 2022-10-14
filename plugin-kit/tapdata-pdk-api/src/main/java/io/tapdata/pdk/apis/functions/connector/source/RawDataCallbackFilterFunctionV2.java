package io.tapdata.pdk.apis.functions.connector.source;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

import java.util.List;
import java.util.Map;

public interface RawDataCallbackFilterFunctionV2 extends TapFunction {
	List<TapEvent> filter(TapConnectorContext context, List<String> tables, Map<String, Object> rawData);
}
