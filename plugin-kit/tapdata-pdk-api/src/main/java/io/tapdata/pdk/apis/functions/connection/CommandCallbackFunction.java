package io.tapdata.pdk.apis.functions.connection;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;
import io.tapdata.pdk.apis.functions.connector.TapConnectionFunction;

public interface CommandCallbackFunction extends TapConnectionFunction {
	/**
	 * CommandInfo is from external viewer, triggered by user interactive. Which defined in json schema in spec.json.
	 * CommandInfo will bring connectionConfig or nodeConfig which are what user input.
	 *
	 * @param context
	 * @param commandInfo
	 * @return the result for external viewer to show back to user
	 */
	CommandResult filter(TapConnectionContext context, CommandInfo commandInfo);
}
