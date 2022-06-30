package io.tapdata.debug;

import com.tapdata.entity.DataQualityTag;
import com.tapdata.entity.Job;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.dataflow.Stage;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DebugUtil {

	private final static String UNKNOWN = "unknown";

	/**
	 * 在用户数据中，添加debug的标记{"__tapd8": {"dataFlowId": "", "stageId": "", "tableName": ""}}
	 *
	 * @param data
	 * @param stageId
	 * @param tableName
	 * @param job
	 */
	public static void addDebugTags(Map<String, Object> data, String stageId, String tableName, Job job) {
		if (MapUtils.isNotEmpty(data)) {

			Map<String, Object> subTapd8;

			if (data.containsKey(DataQualityTag.SUB_COLUMN_NAME)) {
				if (data.get(DataQualityTag.SUB_COLUMN_NAME) instanceof Map) {
					subTapd8 = (Map<String, Object>) data.get(DataQualityTag.SUB_COLUMN_NAME);
				} else {
					return;
				}
			} else {
				subTapd8 = new HashMap<>();
			}

			subTapd8.put(DebugConstant.SUB_DATAFLOW_ID, job.getDataFlowId());
			subTapd8.put(DebugConstant.SUB_STAGE_ID, stageId);
			subTapd8.put(DebugConstant.SUB_TABLE_NAME, tableName);
			data.put(DataQualityTag.SUB_COLUMN_NAME, subTapd8);
		}
	}

	public static String getStageIdFromMessageEntity(MessageEntity msg) {
		if (msg == null
				|| StringUtils.isBlank(msg.getTargetStageId())) return "";

		return msg.getTargetStageId();
	}

	public static void handleStage(Stage stage, String sourceOrTarget) {
		if (stage == null) return;

		if (StringUtils.isBlank(stage.getSourceOrTarget())) {
			stage.setSourceOrTarget(sourceOrTarget);
		}
	}

	public static void handleDebugData(List<Map<String, Object>> datas, ZoneId zoneId) {
		if (CollectionUtils.isEmpty(datas)) return;
		for (Map<String, Object> data : datas) {
			if (MapUtils.isEmpty(data)) continue;

			Map<String, Object> masterData = new HashMap<>();
			Map<String, Object> subd8 = (Map<String, Object>) data.get(DataQualityTag.SUB_COLUMN_NAME);
			if (zoneId == null) {
				zoneId = ZoneId.systemDefault();
			}
			ZoneId finalZoneId = zoneId;
			DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(finalZoneId);
			data.forEach((k, v) -> {
				if (!k.equals(DataQualityTag.SUB_COLUMN_NAME)) {
					if (v instanceof ObjectId) {
						masterData.put(k, ((ObjectId) v).toHexString());
					} else if (v instanceof BigDecimal) {
						masterData.put(k, ((BigDecimal) v).doubleValue());
					} else if (v instanceof Decimal128) {
						masterData.put(k, ((Decimal128) v).bigDecimalValue().doubleValue());
					} else if (v instanceof Instant) {
						masterData.put(k, dateTimeFormatter.format((Instant) v));
					} else if (v instanceof Date) {
						Instant instant = ((Date) v).toInstant();
						masterData.put(k, dateTimeFormatter.format(instant));
					} else {
						masterData.put(k, v);
					}
				}
			});

			data.clear();
			data.put(DataQualityTag.SUB_COLUMN_NAME, subd8);
			data.put("masterData", masterData);
		}
	}

	public static Map<String, Object> constructFileMetaIntoDebugData(Map<String, Object> fileMeta) {
		Map<String, Object> map = new HashMap<>();
		String fileName = fileMeta.get("file_name").toString();
		if (StringUtils.isNotBlank(fileMeta.get("file_extension").toString())) {
			fileName += "." + fileMeta.get("file_extension");
		}
		map.put("file_name", fileName);
		map.put("file_size_ondisk", fileMeta.get("file_size_ondisk"));
		Object fileModifyTimeOndisk = fileMeta.get("file_modify_time_ondisk");
		if (fileModifyTimeOndisk instanceof Long) {
			map.put("file_modify_time_ondisk", new Date((Long) fileModifyTimeOndisk));
		} else {
			map.put("file_modify_time_ondisk", UNKNOWN);
		}
		Object fileCreateTimeOndisk = fileMeta.get("file_create_time_ondisk");
		if (fileCreateTimeOndisk instanceof Long) {
			map.put("file_create_time_ondisk", new Date((Long) fileCreateTimeOndisk));
		} else {
			map.put("file_create_time_ondisk", UNKNOWN);
		}
		map.put("file_path", fileMeta.get("file_path"));
		map.put("source_path", fileMeta.get("source_path"));

		return map;
	}
}
