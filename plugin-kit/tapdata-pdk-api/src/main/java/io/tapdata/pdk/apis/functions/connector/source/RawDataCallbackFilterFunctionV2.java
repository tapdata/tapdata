package io.tapdata.pdk.apis.functions.connector.source;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;

import java.util.List;
import java.util.Map;

public interface RawDataCallbackFilterFunctionV2 extends TapConnectorFunction {
	/**
	 * Tapdata provided rawData queue, this method will receive external rawData and
	 * also receive the offline rawData which offset is managed inside of Engine.
	 *
	 * This method only need provide the conversion from rawData to TapEvent.
	 *
	 * @param context
	 * @param tables which possible the rawData belongs
	 * @param rawData received from external, need convert to
	 *                   TapInsertRecordEvent/TapUpdateRecordEvent/TapDeleteRecordEvent
	 * @return
	 */
	List<TapEvent> filter(TapConnectorContext context, List<String> tables, Map<String, Object> rawData);
}
