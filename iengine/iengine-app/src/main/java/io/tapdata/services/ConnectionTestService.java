package io.tapdata.services;

import com.tapdata.entity.Connections;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.service.skeleton.annotation.RemoteService;
import io.tapdata.websocket.handler.LoadSchemaEventHandler;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.*;

@RemoteService
public class ConnectionTestService {
	public Map<String, Object> connectionTest(Connections connections) {
		return map(entry("1", 1));
	}

	public TapTable testTable(TapTable tapTable) {
		return tapTable;
	}

	public List<LoadSchemaEventHandler.LoadSchemaEvent> getLoadSchemaEvents() {
		LoadSchemaEventHandler.LoadSchemaEvent event = new LoadSchemaEventHandler.LoadSchemaEvent("adf", "fff", table("aa").add(field("aa", "varchar").tapType(tapNumber().bit(32).maxValue(BigDecimal.valueOf(Double.MAX_VALUE)))));
		LoadSchemaEventHandler.LoadSchemaEvent event1 = new LoadSchemaEventHandler.LoadSchemaEvent("adf", "fff", table("bb").add(field("bb", "varchar").tapType(tapString().bytes(32L))));
		return list(event, event1);
	}
}
