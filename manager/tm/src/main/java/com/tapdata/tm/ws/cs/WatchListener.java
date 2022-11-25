/**
 * @title: WatchListener
 * @description:
 * @author lk
 * @date 2021/9/16
 */
package com.tapdata.tm.ws.cs;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.utils.MapUtils;
import com.tapdata.tm.ws.dto.CollectionWatchCache;
import com.tapdata.tm.ws.handler.WatchHandler;
import static com.tapdata.tm.ws.handler.WatchHandler.sendWatchMessage;
import static com.tapdata.tm.ws.handler.WatchHandler.watchCacheMap;

import java.util.HashMap;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.data.mongodb.core.messaging.Message;
import org.springframework.data.mongodb.core.messaging.MessageListener;

@Slf4j
public class WatchListener implements MessageListener<ChangeStreamDocument<Document>, Document> {

	@Override
	public void onMessage(Message<ChangeStreamDocument<Document>, Document> message) {
		log.info("WatchListener received Message in collection: {},message raw: {}, message body:{}",
				message.getProperties().getCollectionName(), message.getRaw(), message.getBody());
		String collectionName = message.getProperties().getCollectionName();
		if (StringUtils.isNotBlank(collectionName)){
			Document body = message.getBody();
			if (watchCacheMap.containsKey(collectionName) && body != null){
				CollectionWatchCache collectionWatchCache = watchCacheMap.get(collectionName);
				if (collectionWatchCache.getReceiverInfoSize() == 0){
					WatchHandler.stopChangeStream(collectionName);
					return;
				}

				Map<String, Map<String, Map<String, String>>> receiverInfo = collectionWatchCache.getReceiverInfo();
				if (MapUtils.isEmpty(receiverInfo)){
					return;
				}
				try {
					new HashMap<>(receiverInfo).entrySet().stream().filter(entry -> body.containsKey(entry.getKey())
							&& entry.getValue().containsKey(body.get(entry.getKey()).toString())).forEach(entry -> {
						entry.getValue().forEach((key, value) -> {
							if (body.get(entry.getKey()).toString().equals(key)) {
								value.values().forEach(receiver -> {
									body.put("id", body.getObjectId("_id").toHexString());
									Map<String, Document> msg = new HashMap<>();
									msg.put("fullDocument", body);
									sendWatchMessage(collectionName, receiver, msg);
								});
							}
						});
					});
				}catch (Exception e){
					log.error("ChangeStream handle message error, body: {},message: {}", JsonUtil.toJson(message.getBody()), e.getMessage());
				}
			}
		}
	}
}
