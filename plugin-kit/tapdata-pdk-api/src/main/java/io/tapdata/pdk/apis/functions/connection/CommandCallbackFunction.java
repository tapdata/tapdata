package io.tapdata.pdk.apis.functions.connection;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.CommandInfo;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

import java.util.Map;

public interface CommandCallbackFunction extends TapFunction {
	CommandResult filter(TapConnectionContext context, CommandInfo commandInfo);
}
