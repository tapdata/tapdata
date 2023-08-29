package io.tapdata.pdk.apis.functions.connector.source;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;

import java.util.List;
import java.util.Map;

@Deprecated
public interface RawDataCallbackFilterFunction extends TapConnectorFunction {
	List<TapEvent> filter(TapConnectorContext context, Map<String, Object> rawData);
}
