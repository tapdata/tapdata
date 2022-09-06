package io.tapdata.pdk.apis.functions.connection;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectionFunction;

import java.util.Map;

public interface CommandCallbackFunction {
	TapEvent filter(TapConnectionContext context, String command, String action, Map<String, Object> argMap);
}
