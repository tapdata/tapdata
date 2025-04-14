package com.tapdata.pdk;

import com.tapdata.constant.HazelcastUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MD5Util;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.task.config.TaskRetryConfig;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.taskinspect.TaskInspectUtils;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.flow.engine.V2.log.LogFactory;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.apis.entity.Projection;
import io.tapdata.pdk.apis.entity.SortOn;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.PdkTableMap;
import io.tapdata.schema.TapTableUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * 节点连接器
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/3/27 15:10 Create
 */
@Slf4j
public class TaskNodePdkConnector implements IPdkConnector {
    private static final String TAG = TaskNodePdkConnector.class.getSimpleName();
    private final Connections connections;
    private final ConnectorNode connectorNode;
    private final QueryByAdvanceFilterFunction queryByAdvanceFilterFunction;
    private final TapCodecsFilterManager codecsFilterManager;
    private final TapCodecsFilterManager defaultCodecsFilterManager;
    private final TaskRetryConfig taskRetryConfig;
    @Getter
    private final String taskId;
    @Getter
    private final String nodeId;

    public TaskNodePdkConnector(
        ClientMongoOperator clientMongoOperator
        , String taskId
        , Node<?> node
        , String associateId
        , Connections connections
        , DatabaseTypeEnum.DatabaseType sourceDatabaseType
        , TaskRetryConfig taskRetryConfig
    ) {
        this.connections = connections;
        this.taskId = taskId;
        this.nodeId = node.getId();
        this.connectorNode = PdkUtil.createNode(
            taskId,
            sourceDatabaseType,
            clientMongoOperator,
            associateId,
            connections.getConfig(),
            new PdkTableMap(TapTableUtil.getTapTableMapByNodeId(TaskInspectUtils.MODULE_NAME, nodeId, System.currentTimeMillis())),
            new PdkStateMap(String.format("%s_%s", TaskInspectUtils.MODULE_NAME, nodeId), HazelcastUtil.getInstance()),
            PdkStateMap.globalStateMap(HazelcastUtil.getInstance()),
            InstanceFactory.instance(LogFactory.class).getLog()
        );
        PDKInvocationMonitor.invoke(connectorNode, PDKMethod.INIT, connectorNode::connectorInit, TAG);

        this.queryByAdvanceFilterFunction = connectorNode.getConnectorFunctions().getQueryByAdvanceFilterFunction();
        this.codecsFilterManager = connectorNode.getCodecsFilterManager();
        this.defaultCodecsFilterManager = TapCodecsFilterManager.create(TapCodecsRegistry.create());
        this.taskRetryConfig = taskRetryConfig;
    }

    @Override
    public TapTable getTapTable(String tableName) {
        return connectorNode.getConnectorContext().getTableMap().get(tableName);
    }

    @Override
    public void eachAllTable(Predicate<TapTable> predicate) {
        Iterator<Entry<TapTable>> iterator = connectorNode.getConnectorContext().getTableMap().iterator();
        while (iterator.hasNext()) {
            Entry<TapTable> table = iterator.next();
            if (!predicate.test(table.getValue())) {
                return;
            }
        }
    }

    @Override
    public LinkedHashMap<String, Object> findOneByKeys(String tableName, LinkedHashMap<String, Object> keys, List<String> fields) {
        TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create();

        // 添加过滤条件
        DataMap match = DataMap.create();
        match.putAll(keys);
        tapAdvanceFilter.match(match);
        tapAdvanceFilter.setLimit(1);

        // 根据主键排序
        tapAdvanceFilter.setSortOnList(keys.keySet()
            .stream()
            .map(fieldName -> new SortOn(fieldName, SortOn.ASCENDING))
            .collect(Collectors.toList())
        );

        // 设置返回字段
        if (!fields.isEmpty()) {
            tapAdvanceFilter.setProjection(Projection.create());
            for (String f : fields) {
                tapAdvanceFilter.getProjection().include(f);
            }
        }

        TapTable tapTable = getTapTable(tableName);
        final AtomicReference<Throwable> throwable = new AtomicReference<>();
        final AtomicReference<LinkedHashMap<String, Object>> data = new AtomicReference<>();
        PDKInvocationMonitor.invoke(connectorNode, PDKMethod.SOURCE_QUERY_BY_ADVANCE_FILTER,
            PDKMethodInvoker.create().runnable(
                    () -> queryByAdvanceFilterFunction.query(connectorNode.getConnectorContext(), tapAdvanceFilter, tapTable, filterResults -> {
                        throwable.set(filterResults.getError());

                        Optional.ofNullable(filterResults.getResults()).ifPresent(results -> {
                            if (results.isEmpty()) return;

                            LinkedHashMap<String, Object> newData = new LinkedHashMap<>();
                            for (Map<String, Object> result : results) {
                                codecsFilterManager.transformToTapValueMap(result, tapTable.getNameFieldMap());
                                defaultCodecsFilterManager.transformFromTapValueMap(result);

                                if (!fields.isEmpty()) {
                                    for (String f : fields) {
                                        newData.put(f, formatValue(result.get(f)));
                                    }
                                } else {
                                    result.forEach((key, val) -> newData.put(key, formatValue(val)));
                                }

                                data.set(newData);
                                return;
                            }
                        });
                    })
                )
                .logTag(TAG)
                .retryPeriodSeconds(taskRetryConfig.getRetryIntervalSecond())
                .maxRetryTimeMinute(taskRetryConfig.getMaxRetryTime(TimeUnit.MINUTES))
        );
        if (null != throwable.get()) {
            throw new RuntimeException(throwable.get());
        }
        return data.get();
    }

    @Override
    public void close() throws Exception {
        if (null != connectorNode) {
            CommonUtils.handleAnyError(() -> {
                PDKInvocationMonitor.invoke(connectorNode, PDKMethod.STOP, connectorNode::connectorStop, TAG);
                log.info("Stop pdk node complete, connection: {}[{}], pdk node: {}", connections.getName(), connections.getId(), connectorNode);
            }, err -> log.warn("stop pdk node failed, connection: {}[{}], pdk node: {}, error: {}\n{}", connections.getName(), connections.getId(), connectorNode, err.getMessage(), Log4jUtil.getStackString(err)));
            CommonUtils.handleAnyError(() -> {
                PDKIntegration.releaseAssociateId(connectorNode.getAssociateId());
                log.info("Release pdk node complete, connection: {}[{}], pdk node: {}", connections.getName(), connections.getId(), connectorNode);
            }, err -> log.warn("Release pdk node failed, connection: {}[{}], pdk node: {}, error: {}\n{}", connections.getName(), connections.getId(), connectorNode, err.getMessage(), Log4jUtil.getStackString(err)));
        }
    }

    private Object formatValue(Object o) {
        if (null == o) return null;

        if (o instanceof DateTime) {
            return ((DateTime) o).toInstant().toString();
        } else if (o instanceof byte[]) {
            return MD5Util.crypt((byte[]) o, false);
//        } else {
//            if (!(o instanceof String
//                || o instanceof Integer
//                || o instanceof Long
//                || o instanceof Float
//                || o instanceof Double
//                || o instanceof BigDecimal
//            )) {
//                log.info("------------- value type: {}", o.getClass());
//            }
        }
        return o;
    }
}
