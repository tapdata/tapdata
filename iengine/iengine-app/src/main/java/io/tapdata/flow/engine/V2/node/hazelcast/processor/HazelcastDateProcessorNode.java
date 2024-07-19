package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.constant.MapUtilV2;
import com.tapdata.constant.TapList;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.DateProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateDateProcessorNode;
import com.tapdata.tm.commons.util.JsonUtil;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.mapping.TypeExprResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.error.TaskDateProcessorExCode_17;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.BiConsumer;


public class HazelcastDateProcessorNode extends HazelcastProcessorBaseNode {

	private static final Logger logger = LogManager.getLogger(HazelcastDateProcessorNode.class);
	public static final String TAG = HazelcastDateProcessorNode.class.getSimpleName();


	/**
	 * 需要修改时间的类型
	 */
	private List<String> dataTypes;
	/**
	 * 增加或者减少
	 */
	private boolean add;
	/**
	 * 增加或者减少的小时数
	 */
	private int hours;

	private DefaultExpressionMatchingMap matchingMap;
	private final Map<String, Map<String, List<String>>> tableFieldMap;

	@SneakyThrows
	public HazelcastDateProcessorNode(ProcessorBaseContext processorBaseContext) {
		super(processorBaseContext);
		initConfig();
		this.tableFieldMap = new HashMap<>();
	}

	private void initConfig() {
		Node node = getNode();
		if (node instanceof DateProcessorNode) {
			this.dataTypes = ((DateProcessorNode) node).getDataTypes();
			this.add = ((DateProcessorNode) node).isAdd();
			this.hours = ((DateProcessorNode) node).getHours();
		} else if (node instanceof MigrateDateProcessorNode) {
			this.dataTypes = ((MigrateDateProcessorNode) node).getDataTypes();
			this.add = ((MigrateDateProcessorNode) node).isAdd();
			this.hours = ((MigrateDateProcessorNode) node).getHours();
		}


		HashMap<String, Map<String, String>> expressions = new HashMap<>();
		for (String dataType : dataTypes) {
			expressions.put(dataType, new HashMap<>());
		}
		matchingMap = DefaultExpressionMatchingMap.map(JsonUtil.toJson(expressions));
	}


	@SneakyThrows
	@Override
	protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		String tableName = TapEventUtil.getTableId(tapEvent);
		ProcessResult processResult = getProcessResult(tableName);

		if (!(tapEvent instanceof TapRecordEvent)) {
			consumer.accept(tapdataEvent, processResult);
			return;
		}

		Node node = getNode();
		boolean syncTask;
		String tableId;
		if (node instanceof DateProcessorNode) {
			tableId = getNode().getId();
			syncTask = true;
		} else {
			tableId = tableName;
			syncTask = false;
		}

		List<String> addTimeFields = tableFieldMap
				.computeIfAbsent(Thread.currentThread().getName(), key -> new HashMap<>())
				.computeIfAbsent(tableId, key -> {
			TapTable tapTable = processorBaseContext.getTapTableMap().get(key);
			if (tapTable == null) {
				throw new TapCodeException(TaskDateProcessorExCode_17.INIT_TARGET_TABLE_TAP_TABLE_NULL, "Table name: " + tableName + "node id: " + getNode().getId());
			}
			LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
			List<String> result = new ArrayList<>();

			if (nameFieldMap != null) {
				nameFieldMap.forEach((k, v) -> {

					String dataType = v.getDataType();
					if (!syncTask) {
						TypeExprResult<DataMap> exprResult = matchingMap.get(v.getDataType());
						if (exprResult != null) {
							dataType = exprResult.getExpression();
						}
					}

					if (dataTypes.contains(dataType)) {
						result.add(k);
					}
				});
			}
			return result;
		});

		addTime(tapEvent, addTimeFields, tableName);
		consumer.accept(tapdataEvent, processResult);
	}

	private void addTime(TapEvent tapEvent, List<String> addTimeFields, String tableName) {
		Map<String, Object> before = TapEventUtil.getBefore(tapEvent);
		if (null != before) {
			addTime(addTimeFields, before, tableName);
		}
		Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
		if (null != after) {
			addTime(addTimeFields, after, tableName);
		}
	}

	private void addTime(List<String> addTimeFields, final Map<String, Object> record, String tableName) {
		for (String addTimeField : addTimeFields) {
			Object valueByKeyV2 = MapUtilV2.getValueByKeyV2(record, addTimeField);
			if (valueByKeyV2 instanceof TapList) {
				((TapList) valueByKeyV2).forEachRealValue(v -> addTime(tableName, addTimeField, v));
			} else {
				addTime(tableName, addTimeField, valueByKeyV2);
			}
		}
	}

	protected void addTime(String tableName, String k, Object v) {
		if (null == v) {
			return;
		}
		DateTime dateTime;
		if (v instanceof TapDateTimeValue) {
			dateTime = ((TapDateTimeValue) v).getValue();
		} else if (v instanceof DateTime) {
			dateTime = (DateTime) v;
		} else {
			throw new TapCodeException(TaskDateProcessorExCode_17.SELECTED_TYPE_IS_NON_TIME, "table: " + tableName
					+ ", key: " + k + ", type: " + v.getClass().getName() + ", value: " + v);
		}
		if (add) {
			dateTime.plus(hours, ChronoUnit.HOURS);
		} else {
			dateTime.minus(hours, ChronoUnit.HOURS);
		}
	}

	@Override
	public boolean needTransformValue() {
		return false;
	}

	@Override
	public boolean supportConcurrentProcess() {
		return true;
	}
}