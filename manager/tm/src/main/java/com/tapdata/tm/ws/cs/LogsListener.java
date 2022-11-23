/**
 * @title: LogsListener
 * @description:
 * @author lk
 * @date 2021/9/11
 */
package com.tapdata.tm.ws.cs;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.utils.MapUtils;
import com.tapdata.tm.ws.dto.LogsCache;
import com.tapdata.tm.ws.handler.LogsHandler;
import static com.tapdata.tm.ws.handler.LogsHandler.logsMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.springframework.data.mongodb.core.messaging.Message;
import org.springframework.data.mongodb.core.messaging.MessageListener;

@Slf4j
public class LogsListener implements MessageListener<ChangeStreamDocument<Document>, Document> {
	@Override
	public void onMessage(Message<ChangeStreamDocument<Document>, Document> message) {
		try {
			if (message.getBody() != null){
				String msg = message.getBody().toJson();
				Map map = JsonUtil.parseJson(msg, Map.class);
				if (MapUtils.isNotEmpty(map)){
					String dataFlowId = MapUtils.getAsStringByPath(map, "contextMap/dataFlowId");
					if (StringUtils.isNotBlank(dataFlowId)){
						if (MapUtils.isEmpty(logsMap)){
							LogsHandler.stopChangeStream();
							return;
						}
						List<LogsCache> logsCaches = logsMap.get(dataFlowId);
						if (CollectionUtils.isEmpty(logsCaches)){
							return;
						}
						for (LogsCache logsCach : new ArrayList<>(logsCaches)) {
							LinkedBlockingQueue<Document> caches = logsCach.getCaches();
							if (logsCach.getEnabled()){
								caches.offer(message.getBody());
							}else {
								if (caches.size() > 0){
									for (int i = 0; i < caches.size(); i++) {
										Document document = caches.poll();
										if (document != null){
											LogsHandler.sendLogsMessage(dataFlowId, logsCach.getReceiver(), Collections.singletonList(document));
										}
									}
								}
								LogsHandler.sendLogsMessage(dataFlowId, logsCach.getReceiver(), Collections.singletonList(message.getBody()));
							}
						}
					}
				}
			}
		}catch (Exception e){
			log.error("ChangeStream handle message error, body: {},message: {}",
					JsonUtil.toJson(message.getBody()), e.getMessage());
		}
	}
}
