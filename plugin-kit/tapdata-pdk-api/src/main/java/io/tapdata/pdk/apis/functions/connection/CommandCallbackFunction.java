package io.tapdata.pdk.apis.functions.connection;

import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.Map;

public interface CommandCallbackFunction {
	Object command(TapConnectionContext context, String command, String action, Map<String, Object> argMap);
}