package io.tapdata.inspect;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.tapdata.constant.JSONUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.MysqlJson;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.apis.functions.PDKMethod;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;

import java.lang.reflect.Array;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/10 7:20 上午
 * @description
 */
public abstract class InspectJob implements Runnable {
	private static final String TAG = InspectJob.class.getSimpleName();
	protected com.tapdata.entity.inspect.InspectTask inspectTask;
	protected String name;
	protected Connections source;
	protected Connections target;
	protected ProgressUpdate progressUpdateCallback;
	protected ConnectorNode sourceNode, targetNode;
	protected String inspectResultParentId;
	protected InspectTaskContext inspectTaskContext;

	public InspectJob(InspectTaskContext inspectTaskContext) {
		this.inspectTask = inspectTaskContext.getTask();
		this.name = inspectTaskContext.getName() + "." + inspectTask.getTaskId();
		this.source = inspectTaskContext.getSource();
		this.target = inspectTaskContext.getTarget();
		this.progressUpdateCallback = inspectTaskContext.getProgressUpdateCallback();
		this.inspectResultParentId = inspectTaskContext.getInspectResultParentId();
		this.sourceNode = inspectTaskContext.getSourceConnectorNode();
		this.targetNode = inspectTaskContext.getTargetConnectorNode();
		this.inspectTaskContext = inspectTaskContext;
		try {
			PDKInvocationMonitor.invoke(sourceNode, PDKMethod.INIT, () -> sourceNode.connectorInit(), TAG);
		} catch (Throwable e) {
			throw new RuntimeException("Init source node failed: " + sourceNode + ", error message: " + e.getMessage(), e);
		}
		try {
			PDKInvocationMonitor.invoke(targetNode, PDKMethod.INIT, () -> targetNode.connectorInit(), TAG);
		} catch (Throwable e) {
			throw new RuntimeException("Init target node failed: " + targetNode + ", error message: " + e.getMessage(), e);
		}
	}

	public static List<String> rdbmsTypes;

	static {
		rdbmsTypes = DatabaseTypeEnum.getRdbmsDatabaseTypes()
				.stream().map(DatabaseTypeEnum::getType).collect(Collectors.toList());
	}

	protected List<String> getSortColumns(String sortColumn) {
		return getSortColumns(sortColumn, ",");
	}

	protected List<String> getSortColumns(String sortColumn, String split) {
		if (StringUtils.isAnyBlank(sortColumn, split)) {
			throw new IllegalArgumentException("Missing input arg: sortColumn, split");
		}

		return Arrays.stream(sortColumn.split(split))
				.filter(column -> StringUtils.isNotBlank(column))
				.collect(Collectors.toList());
	}

	protected static Map<String, Object> diffRecordTypeConvert(Map<String, Object> record, List<String> columns) {
		if (MapUtils.isEmpty(record)) {
			return record;
		}

		Object value;
		Map<String, Object> result = new LinkedHashMap<>();
		for (Map.Entry<String, Object> entry : record.entrySet()) {
			if (null != columns && !columns.isEmpty() && !columns.contains(entry.getKey())) continue;

			value = entry.getValue();
			if (value instanceof Instant) {
				result.put(entry.getKey(), value.toString());
			} else if (value instanceof Date) {
				result.put(entry.getKey(), ((Date) value).toInstant().toString());
			} else if (value instanceof MysqlJson) {
				result.put(entry.getKey(), value.toString());
			} else if (value instanceof Map || value instanceof Array || value instanceof Collection) {
				try {
					JSONUtil.disableFeature(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
					result.put(entry.getKey(), JSONUtil.obj2Json(value));
					JSONUtil.enableFeature(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
				} catch (JsonProcessingException e) {
					e.printStackTrace();
				}
			} else if (value instanceof ObjectId) {
				result.put(entry.getKey(), ((ObjectId) value).toHexString());
			} else if (value instanceof DateTime) {
				result.put(entry.getKey(), ((DateTime) value).toInstant().toString());
			} else {
				result.put(entry.getKey(), entry.getValue());
			}
		}

		return result;
	}

}
