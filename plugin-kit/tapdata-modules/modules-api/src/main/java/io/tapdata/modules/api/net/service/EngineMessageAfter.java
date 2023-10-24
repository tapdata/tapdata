package io.tapdata.modules.api.net.service;

import io.tapdata.pdk.apis.entity.message.EngineMessage;

public interface EngineMessageAfter<T extends EngineMessage> {
	Object intercept(T engineMessage, Object result, Throwable throwable);
}
