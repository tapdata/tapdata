package io.tapdata.autoinspect.connector;

import com.tapdata.constant.HazelcastUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.task.config.TaskRetryConfig;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.autoinspect.connector.IDataCursor;
import com.tapdata.tm.autoinspect.connector.IPdkConnector;
import com.tapdata.tm.autoinspect.constants.AutoInspectConstants;
import com.tapdata.tm.autoinspect.entity.CompareRecord;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.flow.engine.V2.log.LogFactory;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import io.tapdata.flow.engine.V2.util.PdkUtil;
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
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/12 11:31 Create
 */
public class PdkConnector implements IPdkConnector {
    private static final String TAG = PdkConnector.class.getSimpleName();
    private final Logger logger = LogManager.getLogger(PdkConnector.class);
    private final Connections connections;
    private final ConnectorNode connectorNode;
    private final Supplier<Boolean> isRunning;
    private final QueryByAdvanceFilterFunction queryByAdvanceFilterFunction;
    private final TapCodecsFilterManager codecsFilterManager;
    private final TapCodecsFilterManager defaultCodecsFilterManager;
    private final TaskRetryConfig taskRetryConfig;

    public PdkConnector(@NonNull ClientMongoOperator clientMongoOperator, @NonNull String taskId, @NonNull Node node, @NonNull String associateId, @NonNull Connections connections, @NonNull DatabaseTypeEnum.DatabaseType sourceDatabaseType, Supplier<Boolean> isRunning, TaskRetryConfig taskRetryConfig) {
        this.isRunning = isRunning;
        this.connections = connections;
        String nodeId = node.getId();
        ExternalStorageDto pdkStateMapExternalStorage = ExternalStorageUtil.getPdkStateMapExternalStorage(node, connections, clientMongoOperator);
        this.connectorNode = PdkUtil.createNode(
                taskId,
                sourceDatabaseType,
                clientMongoOperator,
                associateId,
                connections.getConfig(),
                new PdkTableMap(TapTableUtil.getTapTableMapByNodeId(AutoInspectConstants.MODULE_NAME, nodeId, System.currentTimeMillis())),
                new PdkStateMap(String.format("%s_%s", AutoInspectConstants.MODULE_NAME, nodeId), HazelcastUtil.getInstance()),
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
    public ObjectId getConnId() {
        return new ObjectId(connections.getId());
    }

    @Override
    public String getName() {
        return connections.getName();
    }

    @Override
    public TapTable getTapTable(String tableName) {
        return connectorNode.getConnectorContext().getTableMap().get(tableName);
    }

    @Override
    public IDataCursor<CompareRecord> queryAll(@NonNull String tableName, Object offset) {
        DataMap ret = new DataMap();
//        if (offset instanceof Map) {
//            ret.putAll((Map)offset);
//        }
        return new InitialPdkCursor(connectorNode, new ObjectId(connections.getId()), tableName, ret, isRunning, taskRetryConfig);
    }

    @Override
    public CompareRecord queryByKey(@NonNull String tableName, @NonNull LinkedHashMap<String, Object> originalKey, @NonNull LinkedHashSet<String> keyNames) {
        TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create();

        // add filter
        DataMap match = DataMap.create();
        match.putAll(originalKey);
        tapAdvanceFilter.match(match);
        tapAdvanceFilter.setLimit(1);

        // sort by primary key
        TapTable tapTable = getTapTable(tableName);
        List<SortOn> sortOnList = new ArrayList<>();
        for (String k : tapTable.primaryKeys()) {
            sortOnList.add(new SortOn(k, SortOn.ASCENDING));
        }
        tapAdvanceFilter.setSortOnList(sortOnList);

        final AtomicReference<Throwable> throwable = new AtomicReference<>();
        final AtomicReference<CompareRecord> compareRecord = new AtomicReference<>();
        PDKInvocationMonitor.invoke(connectorNode, PDKMethod.SOURCE_QUERY_BY_ADVANCE_FILTER,
                PDKMethodInvoker.create()
                        .runnable(
                                () -> queryByAdvanceFilterFunction.query(connectorNode.getConnectorContext(), tapAdvanceFilter, tapTable, filterResults -> {
                                    throwable.set(filterResults.getError());

                                    Optional.ofNullable(filterResults.getResults()).ifPresent(results -> {
                                        if (results.isEmpty()) return;

                                        for (Map<String, Object> result : results) {
                                            codecsFilterManager.transformToTapValueMap(result, tapTable.getNameFieldMap());
                                            defaultCodecsFilterManager.transformFromTapValueMap(result);
                                            CompareRecord record = new CompareRecord(tableName, getConnId(), originalKey, keyNames);
                                            record.setData(result, tapTable.getNameFieldMap());
                                            compareRecord.set(record);
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
        return compareRecord.get();
    }

    @Override
    public void close() throws Exception {
        if (null != connectorNode) {
            CommonUtils.handleAnyError(() -> {
                PDKInvocationMonitor.invoke(connectorNode, PDKMethod.STOP, connectorNode::connectorStop, TAG);
                logger.info("Inspect stop pdk node complete, connection: {}[{}], pdk node: {}", connections.getName(), connections.getId(), connectorNode);
            }, err -> logger.warn("Inspect stop pdk node failed, connection: {}[{}], pdk node: {}, error: {}\n{}", connections.getName(), connections.getId(), connectorNode, err.getMessage(), Log4jUtil.getStackString(err)));
            CommonUtils.handleAnyError(() -> {
                PDKIntegration.releaseAssociateId(connectorNode.getAssociateId());
                logger.info("Inspect release pdk node complete, connection: {}[{}], pdk node: {}", connections.getName(), connections.getId(), connectorNode);
            }, err -> logger.warn("Inspect release pdk node failed,  connection: {}[{}], pdk node: {}, error: {}\n{}", connections.getName(), connections.getId(), connectorNode, err.getMessage(), Log4jUtil.getStackString(err)));
        }
    }
}
