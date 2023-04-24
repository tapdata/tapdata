package io.tapdata.modules.api.net.service;


import io.tapdata.pdk.apis.entity.message.EngineMessage;

import java.util.Map;
import java.util.function.BiConsumer;

public interface EngineMessageExecutionService {
	void callLocal(EngineMessage commandInfo, BiConsumer<Object, Throwable> biConsumer);

	void call(EngineMessage commandInfo, BiConsumer<Object, Throwable> biConsumer);
}
