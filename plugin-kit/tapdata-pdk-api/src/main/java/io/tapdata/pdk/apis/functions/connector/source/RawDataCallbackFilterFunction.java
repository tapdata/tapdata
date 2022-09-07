package io.tapdata.pdk.apis.functions.connector.source;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.Map;

public interface RawDataCallbackFilterFunction {
	TapEvent filter(TapConnectorContext context, Map<String, Object> rawData);
}
