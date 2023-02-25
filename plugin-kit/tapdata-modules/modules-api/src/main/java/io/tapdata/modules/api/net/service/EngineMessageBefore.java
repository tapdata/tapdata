package io.tapdata.modules.api.net.service;

import io.tapdata.pdk.apis.entity.message.EngineMessage;

public interface EngineMessageBefore<T extends EngineMessage> {
	EngineMessageInterceptOptions intercept(T engineMessage);
}
