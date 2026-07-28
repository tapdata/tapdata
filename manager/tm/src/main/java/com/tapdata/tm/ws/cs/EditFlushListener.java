package com.tapdata.tm.ws.cs;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.utils.MapUtils;
import com.tapdata.tm.ws.dto.EditFlushCache;
import com.tapdata.tm.ws.handler.EditFlushHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.BsonDocument;
import org.bson.Document;
import org.springframework.beans.BeanUtils;
import org.springframework.data.mongodb.core.messaging.Message;
import org.springframework.data.mongodb.core.messaging.MessageListener;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;


/**
 * @Author: Zed
 * @Date: 2021/11/25
 * @Description:
 */
@Slf4j
public class EditFlushListener implements MessageListener<ChangeStreamDocument<Document>, TaskEntity> {
	@Override
	public void onMessage(Message<ChangeStreamDocument<Document>, TaskEntity> message) {
		try {
			if (message.getBody() != null){
				TaskEntity taskDtoEntity = message.getBody();

				if (message.getRaw() == null || message.getRaw().getUpdateDescription() == null
						|| message.getRaw().getUpdateDescription().getUpdatedFields() == null) {
					return;
				}


				Set<String> updateFieldNameSet = message.getRaw().getUpdateDescription().getUpdatedFields().keySet();
				boolean updateStatus = UpdateFields.contains(updateFieldNameSet);
				if (!updateStatus) {
					return;
				}

				if (taskDtoEntity != null){
					TaskDto taskDto = new TaskDto();
					entityFromUpdate(message.getRaw().getUpdateDescription().getUpdatedFields(), taskDtoEntity);
					BeanUtils.copyProperties(taskDtoEntity, taskDto);
					String taskId = taskDto.getId().toString();
					if (StringUtils.isNotBlank(taskId)){
						if (MapUtils.isEmpty(EditFlushHandler.editFlushMap)){
							EditFlushHandler.stopChangeStream();
							return;
						}
						List<EditFlushCache> editFlushCaches = EditFlushHandler.editFlushMap.get(taskId);
						if (CollectionUtils.isEmpty(editFlushCaches)){
							return;
						}
						for (EditFlushCache editFlushCache : editFlushCaches) {
//							if (taskDto.getTemp() != null) {
//								EditFlushHandler.sendEditFlushMessage(editFlushCache.getReceiver(), taskId, taskDto.getTemp());
//							}
							EditFlushHandler.sendEditFlushMessage(editFlushCache.getReceiver(), taskId, taskDto);
						}
					}
				}
			}
		}catch (Exception e){
			log.error("ChangeStream handle message error, body: {},message: {}",
					JsonUtil.toJsonUseJackson(message.getBody()), e.getMessage());
		}
	}

	enum UpdateFields {
		TRANSFORMED("transformed", List.of("transformed", "transformUuid")),
		STATUS("status", List.of("status", "agentId", "startTime", "scheduledTime", "schedulingTime", "stoppingTime", "runningTime", "errorTime", "stopTime", "finishTime", "scheduleDate", "stopedDate", "pingTime")),
		LOG_SETTING("logSetting", List.of("logSetting")),
		;
		final String type;
		final List<String> fields;
		UpdateFields(final String type, List<String> fields) {
			this.type = type;
			this.fields = fields;
		}

		public String getType() {
			return type;
		}
		public List<String> getFields() {
			return fields;
		}

		static List<String> allFields(BsonDocument bsonUpdatedFields) {
			List<String> allKeys = new ArrayList<>();
				if (bsonUpdatedFields == null) {
					return allKeys;
				}
			for (UpdateFields value : values()) {
				if (bsonUpdatedFields.containsKey(value.getType())) {
					allKeys.addAll(value.getFields());
				}
			}
				return allKeys;
			}

		public boolean is(String field) {
			return type.equals(field);
		}

		public static boolean contains(Set<String> updateFieldNameSet) {
			for (String name : updateFieldNameSet) {
				for (UpdateFields value : values()) {
					if (value.is(name)) {
						return true;
					}
				}
			}
			return false;
		}
	}

	void entityFromUpdate(BsonDocument bsonUpdatedFields, TaskEntity taskInfo) {
		if (null == bsonUpdatedFields || null == taskInfo) {
			return;
		}
		List<String> allFields = UpdateFields.allFields(bsonUpdatedFields);
		for (String key : allFields) {
			if (!bsonUpdatedFields.containsKey(key)) {
				continue;
			}
			Document updateInfo = Document.parse(new BsonDocument(key, bsonUpdatedFields.get(key)).toJson());
			Object value = updateInfo.get(key);
			try {
				loadVariable(key, value, taskInfo, allFields);
			} catch (Exception e) {
				log.warn("Unable to set update field [" + key + "] into to task info, update field value is: " + JsonUtil.toJson(value), " task info is: " + JsonUtil.toJson(taskInfo));
			}
		}
	}

	void loadVariable(String key, Object value, TaskEntity taskInfo, List<String> allFields) {
		Field keyField = null;
		for (Field field : taskInfo.getClass().getDeclaredFields()) {
			if (allFields.contains(field.getName()) && field.getName().equals(key)) {
				keyField = field;
				keyField.setAccessible(true);
				break;
			}
		}
		if (null == keyField) {
			return;
		}
		ReflectionUtils.setField(keyField, taskInfo, value);
	}
}
