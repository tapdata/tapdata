/**
 * @title: PipeHandler
 * @description:
 * @author lk
 * @date 2021/9/9
 */
package com.tapdata.tm.ws.handler;

import com.alibaba.fastjson.JSON;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.task.service.TaskDagCheckLogService;
import com.tapdata.tm.utils.AES256Util;
import com.tapdata.tm.ws.annotation.WebSocketMessageHandler;
import com.tapdata.tm.ws.dto.MessageInfo;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.enums.MessageType;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;

@WebSocketMessageHandler(type = MessageType.PIPE)
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class PipeHandler implements WebSocketHandler {

	private final MessageQueueService queueService;
	private final DataSourceService dataSourceService;
	private TaskDagCheckLogService taskDagCheckLogService;

	public PipeHandler(MessageQueueService queueService, DataSourceService dataSourceService) {
		this.queueService = queueService;
		this.dataSourceService = dataSourceService;
	}

	@Override
	public void handleMessage(WebSocketContext context) {
		MessageInfo messageInfo = context.getMessageInfo();

		if (StringUtils.isEmpty(messageInfo.getSender())) {
			messageInfo.setSender(context.getSender());
		}

		if (StringUtils.isNotBlank(messageInfo.getReceiver())){
			if (messageInfo.getReceiver().equals(messageInfo.getSender())){
				log.warn("The message ignore,the sender is the same as the receiver");
			}else {

				if (messageInfo.getData() != null && "loadVika".equals(messageInfo.getData().get("type"))) {
					completionDatabasePassword(messageInfo.getData());
				}

				MessageQueueDto messageDto = new MessageQueueDto();
				Optional.ofNullable(messageInfo.getData()).ifPresent(info -> {
					Object error = info.get("error");
					if (Objects.nonNull(error) && (error.toString().contains("404"))) {
						info.put("error", "Please upgrade agent");
					}
				});
				BeanUtils.copyProperties(messageInfo, messageDto);
				queueService.sendMessage(messageDto);
			}
		}else {
			log.warn("WebSocket send message failed, receiver is blank, msg:{}", JSON.toJSONString(messageInfo));
		}
	}

	/**
	 * 补全
	 * @param data
	 */
	private void completionDatabasePassword(Map<String, Object> data) {
		//api_token
		Object obj = data.get("id");
		if (obj != null) {
			Field field = new Field();
			field.put("database_password", true);
			field.put("database_type", true);
			DataSourceConnectionDto connections = dataSourceService.findById(toObjectId(obj.toString()), field);
			String databasePasswordFieldName;
			if (connections != null) {
				switch (connections.getDatabase_type()) {
					case "vika":
						databasePasswordFieldName = "api_token";
						break;
					case "qingflow":
						databasePasswordFieldName = "accessToken";
						break;
					default:
						databasePasswordFieldName = "database_password";
				}
				data.put(databasePasswordFieldName, AES256Util.Aes256Decode(connections.getDatabase_password()));
			}
		}
	}
}
