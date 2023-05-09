package io.tapdata.ddlp.utils;

import com.alibaba.fastjson.JSON;
import com.tapdata.entity.FieldProcess;
import com.tapdata.entity.Mapping;
import com.tapdata.entity.MessageEntity;
import io.tapdata.ddlp.DDLEvent;
import io.tapdata.ddlp.events.AbsField;
import io.tapdata.ddlp.events.AbsStruct;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DDL消息转换工具
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/16 上午4:17 Create
 */
public class DDLMessageEntityUtil {
	protected static final Logger logger = LogManager.getLogger(DDLMessageEntityUtil.class);

	/**
	 * 填充消息体
	 *
	 * @param ddlEvent      DDL事件
	 * @param messageEntity 事件消息
	 */
	public static void fillMessageEntity(DDLEvent ddlEvent, MessageEntity messageEntity) {
		Map<String, Object> cdcEvent = new HashMap<>();
		cdcEvent.put("op", ddlEvent.getOp());
		cdcEvent.put("clz", ddlEvent.getClass().getName());
		cdcEvent.put("json", JSON.toJSONString(ddlEvent));
		messageEntity.setCdcEvent(cdcEvent);
	}

	/**
	 * 转为 DDL 事件
	 *
	 * @param messageEntity 事件消息
	 * @param <T>           事件类型
	 * @return DDL事件实例
	 */
	public static <T extends DDLEvent> T parse(MessageEntity messageEntity) {
		if (null != messageEntity) {
			Map<String, Object> cdcEvent = messageEntity.getCdcEvent();
			if (null != cdcEvent && cdcEvent.containsKey("op") && cdcEvent.containsKey("clz") && cdcEvent.containsKey("json")) {
				Object clzName = cdcEvent.get("clz");
				try {
					Class<T> clz = (Class<T>) Class.forName(clzName.toString());
					return JSON.parseObject(cdcEvent.get("json").toString(), clz);
				} catch (ClassNotFoundException e) {
				}
			}
		}
		return null;
	}

	public static void format(DDLEvent ddlEvent, Mapping mapping, String databaseName, String databaseOwner) {
		if (ddlEvent instanceof AbsStruct) {
			AbsStruct struct = (AbsStruct) ddlEvent;
			String formatTableName = String.join(".", struct.getNamespace());
			struct.setNamespace(Arrays.asList(databaseName, databaseOwner, mapping.getTo_table()));

			if (ddlEvent instanceof AbsField) {
				Map<String, List<FieldProcess>> fieldProcessMap = new HashMap<>();
				List<FieldProcess> fieldsProcess = mapping.getFields_process();
				if (CollectionUtils.isNotEmpty(fieldsProcess)) {
					for (FieldProcess process : fieldsProcess) {
						String field = process.getField();
						if (!fieldProcessMap.containsKey(field)) {
							fieldProcessMap.put(field, new ArrayList<>());
						}

						fieldProcessMap.get(field).add(process);
					}
				}

				AbsField fieldEvent = (AbsField) ddlEvent;
				String columnName = fieldEvent.getName();
				List<FieldProcess> fieldProcessList = fieldProcessMap.get(columnName);
				if (null != fieldProcessList) {
					for (FieldProcess fieldProcess : fieldProcessList) {
						String fieldProcessOp = fieldProcess.getOp();
						if (FieldProcess.FieldOp.OP_RENAME.name().equalsIgnoreCase(fieldProcessOp)) {
							fieldEvent.setName(fieldProcess.getOperand());
						} else if (FieldProcess.FieldOp.OP_REMOVE.name().equalsIgnoreCase(fieldProcessOp)) {
							logger.warn("Found ddl for column {}, but column already remove from table {}, ddl {}.", columnName, formatTableName, struct.getDdl());
						} else if (FieldProcess.FieldOp.OP_CONVERT.name().equalsIgnoreCase(fieldProcessOp)) {
							logger.warn("Found ddl and data type convert for column {}, unsupported both.", columnName);
						}
					}
				}
			}
		}
	}
}
