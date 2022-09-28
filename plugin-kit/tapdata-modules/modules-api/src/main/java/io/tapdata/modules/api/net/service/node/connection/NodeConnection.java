package io.tapdata.modules.api.net.service.node.connection;

import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.modules.api.net.entity.NodeRegistry;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface NodeConnection extends MemoryFetcher {

	void init(NodeRegistry nodeRegistry, BiConsumer<NodeRegistry, String> nodeRegistryReasonSelfDestroy);

	<Request, Response> Response send(String type, Request request, Type responseClass) throws IOException;

	<Request, Response> void sendAsync(String type, Request request, Type responseClass, BiConsumer<Response, Throwable> biConsumer) throws IOException;

	boolean isReady();

	long getTouch();

	void close();
}
