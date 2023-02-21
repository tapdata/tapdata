/**
 * @title: PipeHandler
 * @description:
 * @author lk
 * @date 2021/9/9
 */
package com.tapdata.tm.ws.handler;

import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONValidator;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
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

import java.util.Map;
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

		Map<String, Object> data = messageInfo.getData();
		try {
			Object result = data.get("result");
			String jsonStr = JSON.toJSONString(result);
			JSONValidator jsonValidator = JSONValidator.from(jsonStr);
			jsonValidator.validate();
			if (jsonValidator.getType() == JSONValidator.Type.Object) {
				JSONObject jsonObject = JSON.parseObject(jsonStr);
				if (Objects.nonNull(jsonObject)) {
					log.info("PipeHandler info:{}", jsonStr);
					JSONObject extParam = jsonObject.getJSONObject("extParam");
					if (Objects.nonNull(extParam) && "testConnectionResult".equals(data.get("type").toString())) {
						String taskId = extParam.getString("taskId");
						String templateEnum = extParam.getString("templateEnum");
						String userId = extParam.getString("userId");

						if (StringUtils.isNotBlank(templateEnum)) {
							JSONObject responseBody = jsonObject.getJSONObject("response_body");
							JSONArray validateDetails = responseBody.getJSONArray("validate_details");

							Level grade = ("passed").equals(validateDetails.getJSONObject(0).getString("status")) ? Level.INFO : Level.ERROR;

							String response_body = jsonObject.getJSONObject("response_body").toJSONString();
							taskDagCheckLogService.createLog(taskId, userId, grade, DagOutputTemplateEnum.valueOf(templateEnum),
									true, true, DateUtil.now(), response_body);
						}
					}
				}
			}
		} catch (Exception e) {
			log.warn("PipeHandler handleMessage response body error", e);
		}
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
					if (Objects.nonNull(error) && (error.toString().contains("config") || error.toString().contains("404"))) {
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
