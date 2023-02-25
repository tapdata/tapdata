package io.tapdata.services;

import com.tapdata.entity.Connections;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.service.skeleton.annotation.RemoteService;
import io.tapdata.websocket.handler.LoadSchemaEventHandler;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.*;

@RemoteService
public class ConnectionTestService {
	public Map<String, Object> connectionTest(Connections connections, Boolean needUpdateSchema, Boolean editMode, DataMap context) {
		return context.kv("1", 123);
	}

	public List<TapTable> testTable(TapTable tapTable, TapTable tapTable1, String comment) {
		if(tapTable1 != null)
			tapTable1.setComment(comment);
		return list(tapTable, tapTable1);
	}

	public List<LoadSchemaEventHandler.LoadSchemaEvent> getLoadSchemaEvents() {
		LoadSchemaEventHandler.LoadSchemaEvent event = new LoadSchemaEventHandler.LoadSchemaEvent("adf", "fff", table("aa").add(field("aa", "varchar").tapType(tapNumber().bit(32).maxValue(BigDecimal.valueOf(Double.MAX_VALUE)))));
		LoadSchemaEventHandler.LoadSchemaEvent event1 = new LoadSchemaEventHandler.LoadSchemaEvent("adf", "fff", table("bb").add(field("bb", "varchar").tapType(tapString().bytes(32L))));
		return list(event, event1);
	}

	public void get() {}

}
