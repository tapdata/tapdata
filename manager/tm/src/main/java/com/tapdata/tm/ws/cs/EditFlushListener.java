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
import org.bson.Document;
import org.springframework.beans.BeanUtils;
import org.springframework.data.mongodb.core.messaging.Message;
import org.springframework.data.mongodb.core.messaging.MessageListener;

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

				boolean updateStatus = false;

				for (String f : updateFieldNameSet) {
					if ("status".equals(f) || "transformed".equals(f) || "logSetting".equals(f)) {
						updateStatus = true;
						break;
					}
				}

				if (!updateStatus) {
					return;
				}

				if (taskDtoEntity != null){
					TaskDto taskDto = new TaskDto();
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
}
