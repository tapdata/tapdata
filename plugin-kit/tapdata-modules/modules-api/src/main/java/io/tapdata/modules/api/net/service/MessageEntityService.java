package io.tapdata.modules.api.net.service;

import io.tapdata.modules.api.net.entity.Subscription;
import io.tapdata.modules.api.net.message.MessageEntity;

import java.util.List;

public interface MessageEntityService {
	void save(MessageEntity message);
	void save(List<MessageEntity> messages, ChangedSubscribeIdsListener listener);

	interface ChangedSubscribeIdsListener {
		void changed(List<Subscription> subscriptions);
	}

}
