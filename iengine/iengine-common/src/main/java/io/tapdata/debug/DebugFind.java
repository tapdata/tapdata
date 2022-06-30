package io.tapdata.debug;

import com.tapdata.entity.MessageEntity;

import java.util.List;
import java.util.Map;

public interface DebugFind {

	List<Map<String, Object>> backFindData(List<MessageEntity> msgs) throws DebugException;
}
