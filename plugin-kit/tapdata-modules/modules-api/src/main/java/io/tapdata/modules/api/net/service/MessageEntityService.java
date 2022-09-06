package io.tapdata.modules.api.net.service;

import io.tapdata.modules.api.net.entity.Subscription;
import io.tapdata.modules.api.net.message.MessageEntity;
import io.tapdata.modules.api.proxy.data.FetchNewDataResult;

import java.util.List;

public interface MessageEntityService {
	void save(MessageEntity message);
	void save(List<MessageEntity> messages, ChangedSubscribeIdsListener listener);

	String getOffsetByTimestamp(Long startTime);

	FetchNewDataResult getMessageEntityList(String service, String subscribeId, String offset, Integer limit);

	void remove(String service, String subscribeId);

	interface ChangedSubscribeIdsListener {
		void changed(List<Subscription> subscriptions);
	}

}
