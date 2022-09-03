package io.tapdata.modules.api.net.service;

import io.tapdata.modules.api.net.message.MessageEntity;

import java.util.List;

public interface MessageEntityService {
	void save(List<MessageEntity> messages);

}
