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
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.service.TaskDagCheckLogService;
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

@WebSocketMessageHandler(type = MessageType.PIPE)
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class PipeHandler implements WebSocketHandler {

	private final MessageQueueService queueService;
	private TaskDagCheckLogService taskDagCheckLogService;

	public PipeHandler(MessageQueueService queueService) {
		this.queueService = queueService;
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
					JSONObject extParam = jsonObject.getJSONObject("extParam");
					if (Objects.nonNull(extParam) && "testConnectionResult".equals(data.get("type").toString())) {
						String taskId = extParam.getString("taskId");
						String templateEnum = extParam.getString("templateEnum");
						String userId = extParam.getString("userId");

						if (org.apache.commons.lang3.StringUtils.isNotBlank(templateEnum)) {
							JSONObject responseBody = jsonObject.getJSONObject("response_body");
							JSONArray validateDetails = responseBody.getJSONArray("validate_details");

							String grade = ("passed").equals(validateDetails.getJSONObject(0).getString("status")) ?
									Level.INFO.getValue() : Level.ERROR.getValue();

							taskDagCheckLogService.createLog(taskId, userId, grade, DagOutputTemplateEnum.valueOf(templateEnum),
									true, true, DateUtil.now(), jsonObject.getJSONObject("response_body").toJSONString());
						}
					}
				}
			}
		} catch (Exception e) {
			log.warn("PipeHandler handleMessage response body error", e);
		}

		if (StringUtils.isNotBlank(messageInfo.getReceiver())){
			if (messageInfo.getReceiver().equals(messageInfo.getSender())){
				log.warn("The message ignore,the sender is the same as the receiver");
			}else {
				MessageQueueDto messageDto = new MessageQueueDto();
				BeanUtils.copyProperties(messageInfo, messageDto);
				queueService.sendMessage(messageDto);
			}
		}else {
			log.warn("WebSocket send message failed, receiver is blank, context: {}", JsonUtil.toJson(context));
		}
	}
}
