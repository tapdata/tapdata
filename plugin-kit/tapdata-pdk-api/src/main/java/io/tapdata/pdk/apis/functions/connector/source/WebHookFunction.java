package io.tapdata.pdk.apis.functions.connector.source;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.utils.DataMap;

import java.util.List;
import java.util.function.BiConsumer;

public interface WebHookFunction {
	void hook(DataMap record,
			  Object offsetState,
			  BiConsumer<List<TapEvent>, Object> consumer,
			  String table);
}