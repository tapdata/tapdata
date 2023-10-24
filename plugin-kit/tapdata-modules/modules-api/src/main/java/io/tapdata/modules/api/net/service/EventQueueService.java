package io.tapdata.modules.api.net.service;

import io.tapdata.modules.api.net.message.MessageEntity;

import java.util.List;

public interface EventQueueService {
	void offer(MessageEntity messages);

	void newDataReceived(List<String> subscribeIds);
}
