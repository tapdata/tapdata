package io.tapdata.services;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.service.skeleton.annotation.RemoteService;

import java.util.List;

@RemoteService
public class MemoryService {
	public DataMap memory(List<String> keys, String keyRegex, String memoryLevel) {
		return PDKIntegration.outputMemoryFetchersInDataMap(keys, keyRegex, memoryLevel);
	}
}
