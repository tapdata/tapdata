package io.tapdata.pdk.apis.functions.connector.source;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

import java.util.Map;

public interface RawDataCallbackFilterFunction extends TapFunction {
	TapEvent filter(TapConnectorContext context, Map<String, Object> rawData);
}
