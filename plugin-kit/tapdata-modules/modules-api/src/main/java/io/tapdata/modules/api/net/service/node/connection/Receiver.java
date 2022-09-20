package io.tapdata.modules.api.net.service.node.connection;

public interface Receiver<R, T> {
	R received(String nodeId, T t);
}
