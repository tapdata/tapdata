package io.tapdata.quickapi;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

@TapConnectorClass("spec.json")
public class QuickApiTargetConnector extends QuickApiConnector {
	private static final String TAG = QuickApiConnector.class.getSimpleName();
	private List<String> tables;
	public static final Object syncLock = new Object();

	@Override
	public void onStart(TapConnectionContext connectionContext) {
		getTables();
	}

	@Override
	public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
		if(Objects.nonNull(connectorFunctions)) {
			connectorFunctions.supportWriteRecord(this::write)
					.supportCommandCallbackFunction(this::command)
					.supportErrorHandleFunction(this::errorHandle);
		} else {
			TapLogger.error(TAG,"ConnectorFunctions must be not empty. ");
		}
	}

	private CommandResult command(TapConnectionContext context, CommandInfo info) {

		return null;
	}

	private void write(TapConnectorContext context, List<TapRecordEvent> events, TapTable table, Consumer<WriteListResult<TapRecordEvent>> consumer) {
		if (null == tables){
			getTables();
		}
		if (tables.isEmpty()){
			throw new CoreException("Please describe table in postman json by use tag 'TABLE'.");
		}
		if (null == table || null == table.getId()){
			throw new CoreException("Table can not be empty or tableId can not be empty.");
		}
		String tableId = table.getId();
		if (!tables.contains(tableId)){
			throw new CoreException("Please choice table which has described in postman json by use tag 'TABLE', can not write record to {}", tableId);
		}

		//@TODO write record
	}

	private void getTables(){
		tables = new ArrayList<>();

		//@TODO read table from postman json
	}
}
