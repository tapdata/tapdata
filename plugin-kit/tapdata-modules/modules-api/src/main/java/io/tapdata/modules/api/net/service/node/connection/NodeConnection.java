package io.tapdata.modules.api.net.service.node.connection;

import io.tapdata.modules.api.net.entity.NodeRegistry;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface NodeConnection {

	void init(NodeRegistry nodeRegistry, BiConsumer<NodeRegistry, String> nodeRegistryReasonSelfDestroy);

	<Request, Response> Response send(String type, Request request, Type responseClass) throws IOException;

	boolean isReady();

	long getTouch();

	void close();
}
