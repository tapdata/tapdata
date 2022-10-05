package io.tapdata.modules.api.net.service.node.connection;

import java.util.function.BiConsumer;

public interface Receiver<R, T> {
	void received(String nodeId, T t, BiConsumer<Object, Throwable> biConsumer);
}
