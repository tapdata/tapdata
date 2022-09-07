package io.tapdata.pdk.apis.functions.connection;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.CommandInfo;

import java.util.Map;

public interface CommandCallbackFunction {
	Map<String, Object> filter(TapConnectionContext context, CommandInfo commandInfo);
}
