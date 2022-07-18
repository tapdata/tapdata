package io.tapdata.common;

import com.tapdata.entity.MessageEntity;

import java.util.List;

public interface DdlHandler {

	List<MessageEntity> handleMessage(List<MessageEntity> messageEntities);

}
