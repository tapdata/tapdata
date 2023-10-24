package io.tapdata.modules.api.service;

import java.util.concurrent.CompletableFuture;

public interface SkeletonService {
	String SERVICE_ENGINE = "tapdata_engine";
	CompletableFuture<Object> call(String className, String method, Object... args);
	<T> CompletableFuture<T> call(String className, String method, Class<T> responseClass, Object... args);
}
