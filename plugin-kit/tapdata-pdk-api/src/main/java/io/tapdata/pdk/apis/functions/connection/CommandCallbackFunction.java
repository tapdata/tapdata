package io.tapdata.pdk.apis.functions.connection;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.CommandInfo;
import io.tapdata.pdk.apis.entity.CommandResult;

import java.util.Map;

public interface CommandCallbackFunction {
	CommandResult filter(TapConnectionContext context, CommandInfo commandInfo);
}
