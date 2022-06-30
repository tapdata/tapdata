package com.tapdata.constant;

import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.SyncStageEnum;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2020-11-21 14:32
 **/
public class OffsetUtil {

	public static String getSyncStage(Object offset) {
		if (offset == null) {
			return SyncStageEnum.SNAPSHOT.getSyncStage();
		}
		if (offset instanceof TapdataOffset) {
			return ((TapdataOffset) offset).getSyncStage();
		} else if (offset instanceof Map) {
			if (((Map) offset).containsKey("syncStage")) {
				return ((Map) offset).get("syncStage").toString();
			} else {
				if (((Map<?, ?>) offset).containsKey("snapshotOffset") && Boolean.parseBoolean(((Map<?, ?>) offset).get("snapshotOffset").toString())) {
					return SyncStageEnum.SNAPSHOT.getSyncStage();
				} else {
					return MapUtils.isNotEmpty((Map) offset) ? SyncStageEnum.CDC.getSyncStage() : SyncStageEnum.SNAPSHOT.getSyncStage();
				}
			}
		} else {
			return SyncStageEnum.CDC.getSyncStage();
		}
	}

	public static String getSyncStage(MessageEntity messageEntity) {
		return messageEntity != null ? getSyncStage(messageEntity.getOffset()) : "";
	}

	public static String getSyncStage(List<MessageEntity> messageEntityList) {
		return CollectionUtils.isNotEmpty(messageEntityList) ? getSyncStage(messageEntityList.get(0)) : "";
	}

}
