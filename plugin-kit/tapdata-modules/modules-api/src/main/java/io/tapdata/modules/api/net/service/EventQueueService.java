package io.tapdata.modules.api.net.service;

import io.tapdata.modules.api.net.message.MessageEntity;

import java.util.List;

public interface EventQueueService {
	void offer(List<MessageEntity> messages);
	void subscribe(List<String> connectionIds, String nodeId);
	void unsubscribe(List<String> connectionIds, String nodeId);
}
