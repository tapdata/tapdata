package io.tapdata.modules.api.net.service.node.connection;

import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.modules.api.net.entity.NodeRegistry;
import io.tapdata.entity.tracker.MessageTracker;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.function.BiConsumer;

public interface NodeConnection extends MemoryFetcher {

	void init(NodeRegistry nodeRegistry, BiConsumer<NodeRegistry, String> nodeRegistryReasonSelfDestroy);

	<Request extends MessageTracker, Response> Response send(String type, Request request, Type responseClass) throws IOException;

	<Request extends MessageTracker, Response> void sendAsync(String type, Request request, Type responseClass, BiConsumer<Response, Throwable> biConsumer) throws IOException;

	boolean isReady();

	long getTouch();

	void close();

	String getWorkingIpPort();
	String getId();
}
