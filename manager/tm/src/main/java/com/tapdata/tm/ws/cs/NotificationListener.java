/**
 * @title: MessageListener
 * @description:
 * @author lk
 * @date 2021/9/22
 */
package com.tapdata.tm.ws.cs;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.utils.MapUtils;
import com.tapdata.tm.ws.handler.NotificationHandler;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.data.mongodb.core.messaging.Message;
import org.springframework.data.mongodb.core.messaging.MessageListener;

import java.util.HashMap;
import java.util.Map;

import static com.tapdata.tm.ws.handler.NotificationHandler.notificationMap;

/**
 * 监控message表的变动情况，有变动，就会触发onMessage
 */
@Slf4j
public class NotificationListener implements MessageListener<ChangeStreamDocument<Document>, Document> {

    @Override
    public void onMessage(Message<ChangeStreamDocument<Document>, Document> message) {
        try {
            if (message.getBody() != null) {
                Document document = message.getBody();
                log.info("NotificationListener  msg:{}", document);
                String userId = (String) document.get("user_id");
                if (StringUtils.isNotBlank(userId)) {
                    if (MapUtils.isEmpty(notificationMap)) {
                        NotificationHandler.stopChangeStream();
                        return;
                    }
                    Map<String, String> map = notificationMap.get(userId);
                    if (MapUtils.isNotEmpty(map)) {
                        new HashMap<>(map).forEach((key, value) -> NotificationHandler.sendNotification(value,document));
                    }
                } else {
                    log.info("NotificationListener  messageEntity has no userId:{}", JsonUtil.toJsonUseJackson(document));
                }
            }
        } catch (Exception e) {
            log.error("ChangeStream handle message error,message: {}", e.getMessage());
        }
    }
}
