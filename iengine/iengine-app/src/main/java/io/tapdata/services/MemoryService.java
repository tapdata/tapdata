package io.tapdata.services;

import com.tapdata.entity.Connections;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.service.skeleton.annotation.RemoteService;
import io.tapdata.websocket.handler.LoadSchemaEventHandler;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.*;

@RemoteService
public class MemoryService {
	public DataMap memory(List<String> keys, String keyRegex, String memoryLevel) {
		return PDKIntegration.outputMemoryFetchersInDataMap(keys, keyRegex, memoryLevel);
	}
}
