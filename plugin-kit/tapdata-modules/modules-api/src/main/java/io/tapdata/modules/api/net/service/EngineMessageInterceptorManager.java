package io.tapdata.modules.api.net.service;

public interface EngineMessageInterceptorManager {
	EngineMessageInterceptorManager registerBefore(String matchingKey, EngineMessageAfter interceptor);
	EngineMessageInterceptorManager registerAfter(String matchingKey, EngineMessageAfter interceptor);
	void unregister(String matchingKey, EngineMessageAfter interceptor);
}
