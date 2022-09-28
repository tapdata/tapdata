package io.tapdata.modules.api.net.service.node.connection;

import io.tapdata.entity.memory.MemoryFetcher;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public interface NodeConnectionFactory extends MemoryFetcher {
	NodeConnection getNodeConnection(String nodeId);

	boolean isDisconnected(String nodeId);

	NodeConnection removeNodeConnection(String nodeId);

	<Request, Response> void registerReceiver(String type, Receiver<Response, Request> receiver);

	void received(String nodeId, String type, Byte encode, byte[] data, BiConsumer<Object, Throwable> biConsumer);
}
