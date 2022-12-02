package io.tapdata.autoinspect.connector;

import com.tapdata.entity.task.config.TaskRetryConfig;
import com.tapdata.tm.autoinspect.connector.IDataCursor;
import com.tapdata.tm.autoinspect.entity.CompareRecord;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.entity.Projection;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.entity.SortOn;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.apis.functions.PDKMethod;
import lombok.NonNull;
import org.bson.types.ObjectId;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/9 10:43 Create
 */
public class InitialPdkCursor implements IDataCursor<CompareRecord> {
    private static final String TAG = InitialPdkCursor.class.getSimpleName();
    private static final int BATCH_SIZE = 500;
    private static final boolean fullMatch = true;

    private Throwable throwable;
    private final ConnectorNode connectorNode;
    private final QueryByAdvanceFilterFunction queryByAdvanceFilterFunction;
    private final TapCodecsFilterManager codecsFilterManager;
    private final TapCodecsFilterManager defaultCodecsFilterManager;

    private final LinkedBlockingQueue<CompareRecord> queue = new LinkedBlockingQueue<>();
    private final AtomicBoolean hasNext = new AtomicBoolean(true);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final @NonNull String tableName;
    private final @NonNull ObjectId connectionId;
    private final @NonNull TapTable tapTable;
    private final @NonNull DataMap offset;
    private Projection projection;
    private final List<SortOn> sortOnList = new ArrayList<>();
    private final Supplier<Boolean> isRunning;
    private final TaskRetryConfig taskRetryConfig;

    public InitialPdkCursor(@NonNull ConnectorNode connectorNode, @NonNull ObjectId connectionId, @NonNull String tableName, @NonNull DataMap offset, @NonNull Supplier<Boolean> isRunning, TaskRetryConfig taskRetryConfig) {
        this.isRunning = () -> !closed.get() && isRunning.get();
        this.tableName = tableName;
        this.connectionId = connectionId;
        this.offset = offset;
        this.connectorNode = connectorNode;
        this.queryByAdvanceFilterFunction = connectorNode.getConnectorFunctions().getQueryByAdvanceFilterFunction();
        this.codecsFilterManager = connectorNode.getCodecsFilterManager();
        this.defaultCodecsFilterManager = TapCodecsFilterManager.create(TapCodecsRegistry.create());

        this.tapTable = connectorNode.getConnectorContext().getTableMap().get(tableName);
        this.taskRetryConfig = taskRetryConfig;
        for (String k : tapTable.primaryKeys()) {
            sortOnList.add(new SortOn(k, SortOn.ASCENDING));
        }

        if (!fullMatch) {
            projection = new Projection();
            for (SortOn sortOn : sortOnList) {
                projection.include(sortOn.getKey());
            }
        }
    }

    @Override
    public CompareRecord next() throws Exception {
        if (hasNext.get() && queue.size() == 0) {
            queryNextBatch();
        }
        while (isRunning.get()) {
            try {
                CompareRecord record = queue.poll(100L, TimeUnit.MILLISECONDS);
                if (null != record) {
                    return record;
                }

                if (!hasNext.get()) break;
            } catch (InterruptedException e) {
                break;
            }
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        closed.set(true);
        queue.clear();
    }

    private void queryNextBatch() throws Exception {
        TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create();

        if (!offset.isEmpty()) {
            List<QueryOperator> operators = new LinkedList<>();
            offset.forEach((k, v) -> operators.add(QueryOperator.gt(k, v)));
            tapAdvanceFilter.setOperators(operators);
        }

        tapAdvanceFilter.setLimit(BATCH_SIZE);
        tapAdvanceFilter.setSortOnList(sortOnList);
        tapAdvanceFilter.setProjection(projection);

        PDKInvocationMonitor.invoke(connectorNode, PDKMethod.SOURCE_QUERY_BY_ADVANCE_FILTER,
                PDKMethodInvoker.create().runnable(
                                () -> queryByAdvanceFilterFunction.query(connectorNode.getConnectorContext(), tapAdvanceFilter, tapTable, filterResults -> {
                                    Throwable error = filterResults.getError();
                                    if (null != error) {
                                        throwable = error;
                                        hasNext.set(false);
                                        return;
                                    }

                                    hasNext.set(Optional.ofNullable(filterResults.getResults()).map(results -> {
                                        if (results.isEmpty()) return false;

                                        for (Map<String, Object> result : results) {
                                            codecsFilterManager.transformToTapValueMap(result, tapTable.getNameFieldMap());
                                            defaultCodecsFilterManager.transformFromTapValueMap(result);
                                            CompareRecord record = new CompareRecord(tableName, connectionId);
                                            record.setData(result, tapTable.getNameFieldMap());
                                            for (SortOn s : sortOnList) {
                                                record.getKeyNames().add(s.getKey());
                                                record.getOriginalKey().put(s.getKey(), result.get(s.getKey()));
                                            }

                                            while (true) {
                                                if (!isRunning.get()) return false;
                                                try {
                                                    if (queue.offer(record, 100L, TimeUnit.MILLISECONDS)) {
                                                        sortOnList.forEach(s -> offset.put(s.getKey(), result.get(s.getKey())));
                                                        break;
                                                    }
                                                } catch (InterruptedException e) {
                                                    return false;
                                                }
                                            }
                                        }
                                        return true;
                                    }).orElse(false));
                                })
                        ).logTag(TAG)
                        .retryPeriodSeconds(taskRetryConfig.getRetryIntervalSecond())
                        .maxRetryTimeMinute(taskRetryConfig.getMaxRetryTime(TimeUnit.MINUTES))
        );
        if (throwable instanceof Exception) {
            throw (Exception) throwable;
        } else if (null != throwable) {
            throw new Exception(throwable);
        }

        if (queue.size() == 0) {
            hasNext.set(false);
        }
    }
}
