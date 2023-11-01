package io.tapdata.inspect.compare;

import cn.hutool.core.map.MapUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.inspect.InspectDataSource;
import com.tapdata.entity.inspect.InspectStatus;
import com.tapdata.tm.commons.util.MetaType;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.flow.engine.V2.exception.node.NodeException;
import io.tapdata.inspect.InspectJob;
import io.tapdata.inspect.InspectTaskContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.apis.functions.connector.source.CountByPartitionFilterFunction;
import io.tapdata.pdk.apis.functions.connector.source.ExecuteCommandFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeanUtils;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * table rows inspect task
 *
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/10 8:34 上午
 * @description
 */
public class TableRowCountInspectJob extends InspectJob {
	private static final String TAG = TableRowCountInspectJob.class.getSimpleName();
	private static Logger logger = LogManager.getLogger(TableRowCountInspectJob.class);

	public TableRowCountInspectJob(InspectTaskContext inspectTaskContext) {
		super(inspectTaskContext);
	}

	@Override
	protected void doRun() {
		try {
			int retry = 0;
			while (retry < 4) {
				try {
					AtomicLong sourceCount = new AtomicLong();
					AtomicLong targetCount = new AtomicLong();

					List<QueryOperator> srcConditions = inspectTask.getSource().getConditions();
					TapTable srcTable = getTapTable(inspectTask.getSource());
					if (CollectionUtils.isNotEmpty(srcConditions) && null != inspectTask.getSource().getIsFilter() && inspectTask.getSource().getIsFilter()) {
						CountByPartitionFilterFunction srcCountByPartitionFilterFunction = this.sourceNode.getConnectorFunctions().getCountByPartitionFilterFunction();
						if (null == srcCountByPartitionFilterFunction) {
							retry = 3;
							throw new RuntimeException("Source node does not support count with filter function: " + sourceNode.getConnectorContext().getSpecification().getId());
						}
						TapAdvanceFilter tapAdvanceFilter = wrapFilter(srcConditions);
						PDKInvocationMonitor.invoke(this.sourceNode, PDKMethod.COUNT_BY_PARTITION_FILTER,
								() -> sourceCount.set(srcCountByPartitionFilterFunction.countByPartitionFilter(this.sourceNode.getConnectorContext(), srcTable, tapAdvanceFilter)), TAG);
					} else if (inspectTask.getSource().isEnableCustomCommand() && MapUtil.isNotEmpty(inspectTask.getSource().getCustomCommand())) {
						ExecuteCommandFunction executeCommandFunction = this.sourceNode.getConnectorFunctions().getExecuteCommandFunction();
						if (null == executeCommandFunction) {
							retry = 3;
							throw new RuntimeException("Source node does not support execute command function: " + sourceNode.getConnectorContext().getSpecification().getId());
						}
						Map<String, Object> customCountCommand = setCommandCountParam(inspectTask.getSource().getCustomCommand(),this.sourceNode,srcTable);
						TapExecuteCommand tapExecuteCommand = TapExecuteCommand.create()
								.command((String) customCountCommand.get("command")).params((Map<String, Object>) customCountCommand.get("params"));
						List<Map<String, Object>> maps = executeCommand(executeCommandFunction, tapExecuteCommand,this.sourceNode);
						long count = 0l;
						if (CollectionUtils.isNotEmpty(maps)) {
							count = maps.get(0).values().stream().mapToLong(value -> Long.parseLong(value.toString())).sum();
						}
						sourceCount.set(count);

					} else {
						BatchCountFunction srcBatchCountFunction = this.sourceNode.getConnectorFunctions().getBatchCountFunction();
						if (null == srcBatchCountFunction) {
							retry = 3;
							throw new RuntimeException("Source node does not support batch count function: " + sourceNode.getConnectorContext().getSpecification().getId());
						}
						PDKInvocationMonitor.invoke(this.sourceNode, PDKMethod.SOURCE_BATCH_COUNT,
								() -> sourceCount.set(srcBatchCountFunction.count(this.sourceNode.getConnectorContext(), srcTable)),
								TAG
						);
					}
					List<QueryOperator> tgtConditions = inspectTask.getTarget().getConditions();
					TapTable tgtTable = getTapTable(inspectTask.getTarget());
					if (CollectionUtils.isNotEmpty(tgtConditions) && null != inspectTask.getTarget().getIsFilter() && inspectTask.getTarget().getIsFilter()) {
						CountByPartitionFilterFunction tgtCountByPartitionFilterFunction = this.targetNode.getConnectorFunctions().getCountByPartitionFilterFunction();
						if (null == tgtCountByPartitionFilterFunction) {
							retry = 3;
							throw new RuntimeException("Target node does not support count with filter function: " + targetNode.getConnectorContext().getSpecification().getId());
						}
						TapAdvanceFilter tapAdvanceFilter = wrapFilter(tgtConditions);
						PDKInvocationMonitor.invoke(this.targetNode, PDKMethod.COUNT_BY_PARTITION_FILTER,
								() -> targetCount.set(tgtCountByPartitionFilterFunction.countByPartitionFilter(this.targetNode.getConnectorContext(), tgtTable, tapAdvanceFilter)), TAG);
					} else if (inspectTask.getTarget().isEnableCustomCommand() && MapUtil.isNotEmpty(inspectTask.getTarget().getCustomCommand())) {
						ExecuteCommandFunction executeCommandFunction = this.targetNode.getConnectorFunctions().getExecuteCommandFunction();
						if (null == executeCommandFunction) {
							retry = 3;
							throw new RuntimeException("Source node does not support execute command function: " + targetNode.getConnectorContext().getSpecification().getId());
						}
						Map<String, Object> customCountCommand = setCommandCountParam(inspectTask.getTarget().getCustomCommand(), this.targetNode, tgtTable);
						TapExecuteCommand tapExecuteCommand = TapExecuteCommand.create()
								.command((String) customCountCommand.get("command")).params((Map<String, Object>) customCountCommand.get("params"));
						List<Map<String, Object>> maps = executeCommand(executeCommandFunction, tapExecuteCommand,this.targetNode);
						long count = 0l;
						if (CollectionUtils.isNotEmpty(maps)) {
							count = maps.get(0).values().stream().mapToLong(value -> Long.parseLong(value.toString())).sum();
						}
						targetCount.set(count);
					}else {
						BatchCountFunction tgtBatchCountFunction = this.targetNode.getConnectorFunctions().getBatchCountFunction();
						if (null == tgtBatchCountFunction) {
							retry = 3;
							throw new RuntimeException("Target node does not support batch count function: " + targetNode.getConnectorContext().getSpecification().getId());
						}
						PDKInvocationMonitor.invoke(this.targetNode, PDKMethod.SOURCE_BATCH_COUNT,
								() -> targetCount.set(tgtBatchCountFunction.count(this.targetNode.getConnectorContext(), tgtTable)),
								TAG
						);
					}

					boolean passed = sourceCount.get() == targetCount.get();

					stats.setEnd(new Date());
					stats.setStatus("done");
					stats.setResult(passed ? "passed" : "failed");
					stats.setProgress(1);
					stats.setSource_only(sourceCount.get());
					stats.setTarget_only(targetCount.get());
					stats.setSource_total(sourceCount.get());
					stats.setTarget_total(targetCount.get());
					break;
				} catch (Exception e) {
					if (retry >= 3) {
						logger.error(String.format("Failed to compare the count of rows in table %s.%s and table %s.%s, the taskId is %s",
								source.getName(), inspectTask.getSource().getTable(),
								target.getName(), inspectTask.getTarget().getTable(), inspectTask.getTaskId()), e);
						stats.setEnd(new Date());
						stats.setStatus(InspectStatus.ERROR.getCode());
						stats.setResult("failed");
						stats.setErrorMsg(e.getMessage() + "\n" +
								Arrays.stream(e.getStackTrace()).map(StackTraceElement::toString).collect(Collectors.joining("\n")));
						break;
					}
					retry++;
					stats.setErrorMsg(String.format("Check has an exception and is trying again..., The number of retries: %s", retry));
					stats.setStatus(InspectStatus.ERROR.getCode());
					stats.setEnd(new Date());
					stats.setResult("failed");
					progressUpdateCallback.progress(inspectTask, stats, null);
					logger.error(String.format("Check has an exception and is trying again..., The number of retries: %s", retry), e);
					try {
						TimeUnit.SECONDS.sleep(5);
					} catch (InterruptedException interruptedException) {
						break;
					}
				}
			}
		} catch (Throwable e) {
			logger.error("Inspect failed " + name, e);
		}
	}

	@NotNull
	private static TapAdvanceFilter wrapFilter(List<QueryOperator> srcConditions) {
		TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create();
		tapAdvanceFilter.setOperators(srcConditions);
		DataMap match = new DataMap();
		if (null != srcConditions) {
			srcConditions.stream().filter(op->op.getOperator()== 5).forEach(op->match.put(op.getKey(), op.getValue()));
		}
		tapAdvanceFilter.setMatch(match);
		return tapAdvanceFilter;
	}

	private TapTable getTapTable(InspectDataSource inspectDataSource) {
		Map<String, Object> params = new HashMap<>();
		params.put("connectionId", inspectDataSource.getConnectionId());
		params.put("metaType", MetaType.table.name());
		params.put("tableName", inspectDataSource.getTable());
		TapTable tapTable = inspectTaskContext.getClientMongoOperator().findOne(params, ConnectorConstant.METADATA_INSTANCE_COLLECTION + "/metadata/v2", TapTable.class);
		if (null == tapTable) {
			tapTable = new TapTable(inspectDataSource.getTable());
		}
		return tapTable;
	}


	public static List<Map<String, Object>> executeCommand(ExecuteCommandFunction executeCommandFunction, TapExecuteCommand tapExecuteCommand,
														   ConnectorNode node) {
		AtomicReference<List<Map<String, Object>>> maps = new AtomicReference<>();
		PDKInvocationMonitor.invoke(node, PDKMethod.COUNT_BY_PARTITION_FILTER,
				() -> executeCommandFunction.execute(node.getConnectorContext(), tapExecuteCommand, executeResult -> {
					if (executeResult.getError() != null) {
						throw new NodeException("Execute error: " + executeResult.getError().getMessage(), executeResult.getError());
					}
					if (executeResult.getResult() == null) {
						logger.info("Execute result is null");
					} else {
						if (executeResult.getResult() instanceof Long) {
							List<Map<String, Object>> countList = new ArrayList<>();
							Map<String, Object> map = new LinkedHashMap<>();
							map.put("count",executeResult.getResult());
							countList.add(map);
							maps.set(countList);
						} else{
							maps.set((List<Map<String, Object>>) executeResult.getResult());
					}
					}
				}), TAG);

		return maps.get();
	}


	public static Map<String, Object> setCommandCountParam(Map<String, Object> customCommand,ConnectorNode node,TapTable tgtTable) {
		Map<String, Object> copyCustomCommand = new LinkedHashMap<>();
		com.tapdata.constant.MapUtil.copyToNewMap(customCommand,copyCustomCommand);
		Map<String, Object> params = (Map<String, Object>) copyCustomCommand.get("params");
		if(!node.getTapNodeInfo().getTapNodeSpecification().getId().contains("mongodb")) {
			Object value = params.get("sql");
			if (value != null) {
				String sql = getCountSql(value.toString());
				params.put("sql", sql);
			}
		}else {
			copyCustomCommand.put("command","count");
			params.put("collection",tgtTable.getId());
		}
		return copyCustomCommand;
	}

	public static String getCountSql(String customSql) {
		String sql = customSql.trim().replaceAll("[\t\n\r]", "");
		// remove order by
		sql = sql.replaceAll("\\s+[Oo][Rr][Dd][Ee][Rr]\\s[Bb][Yy].+", "");
		Pattern groupByPattern = Pattern.compile(".+[Gg][Rr][Oo][Uu][Pp]\\s[Bb][Yy].+");
		if (groupByPattern.matcher(sql).matches()) {
			// has group by
			sql = "SELECT COUNT(1) FROM (" + sql + ")";
		} else {
			sql = sql.replaceAll("[Ss][Ee][Ll][Ee][Cc][Tt].+[Ff][Rr][Oo][Mm]", "SELECT COUNT(1) FROM");
		}
		return sql.trim();
	}
}
