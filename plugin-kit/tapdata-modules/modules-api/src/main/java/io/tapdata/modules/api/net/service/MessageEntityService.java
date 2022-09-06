package io.tapdata.modules.api.net.service;

import io.tapdata.modules.api.net.entity.Subscription;
import io.tapdata.modules.api.net.message.MessageEntity;
import io.tapdata.modules.api.proxy.data.FetchNewDataResult;

import java.util.List;

public interface MessageEntityService {
	void save(MessageEntity message);
	void save(List<MessageEntity> messages, ChangedSubscribeIdsListener listener);

	Object getOffsetByTimestamp(Long startTime);

	FetchNewDataResult getMessageEntityList(String service, String subscribeId, Object offset, Integer limit);

	interface ChangedSubscribeIdsListener {
		void changed(List<Subscription> subscriptions);
	}

}
