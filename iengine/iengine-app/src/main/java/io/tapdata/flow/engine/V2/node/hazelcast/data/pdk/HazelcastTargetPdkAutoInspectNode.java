package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.alibaba.fastjson.JSON;
import com.tapdata.constant.BeanUtil;
import com.tapdata.entity.TapdataShareLogEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.autoinspect.compare.IAutoCompare;
import com.tapdata.tm.autoinspect.connector.IPdkConnector;
import com.tapdata.tm.autoinspect.constants.TaskType;
import com.tapdata.tm.autoinspect.dto.TaskAutoInspectResultDto;
import com.tapdata.tm.autoinspect.entity.AutoInspectProgress;
import com.tapdata.tm.autoinspect.entity.CompareRecord;
import com.tapdata.tm.autoinspect.entity.CompareTableItem;
import com.tapdata.tm.autoinspect.utils.AutoInspectUtil;
import com.tapdata.tm.commons.dag.nodes.AutoInspectNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.autoinspect.runner.PdkAutoInspectRunner;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.module.api.PipelineDelay;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/17 16:47 Create
 */
public class HazelcastTargetPdkAutoInspectNode extends HazelcastTargetPdkBaseNode {

    private static final Logger logger = LogManager.getLogger(HazelcastTargetPdkAutoInspectNode.class);
    private static final ExecutorService EXECUTORS = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<>());
    private final AtomicBoolean isInitialed = new AtomicBoolean(false);
    private final AtomicLong lastReferenceTimes = new AtomicLong();
    private final AtomicLong processDelay = new AtomicLong();
    private final PipelineDelay pipelineDelay = InstanceFactory.instance(PipelineDelay.class);
    private final LinkedBlockingQueue<List<TapEvent>> incrementQueue = new LinkedBlockingQueue<>(1000);//todo: Optimize the queue, restart task maybe lost events

    private final ObsLogger userLogger;
    private final String dataNodeId;

    public HazelcastTargetPdkAutoInspectNode(DataProcessorContext dataProcessorContext) {
        super(dataProcessorContext);

        if (!(dataProcessorContext.getNode() instanceof AutoInspectNode)) {
            throw new RuntimeException("Not AutoInspectNode instance");
        }
        AutoInspectNode node = (AutoInspectNode) dataProcessorContext.getNode();
        this.dataNodeId = node.getToNode().getId();

        TaskDto task = dataProcessorContext.getTaskDto();
        String taskId = task.getId().toHexString();
        TaskType taskType = TaskType.parseByTaskType(syncType.getSyncType());
        AutoInspectProgress progress = AutoInspectUtil.parse(task.getAttrs());
        ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);

        userLogger = ObsLoggerFactory.getInstance().getObsLogger(task, node.getId(), node.getName());

        HazelcastTargetPdkAutoInspectNode _thisNode = this;
        EXECUTORS.submit(new PdkAutoInspectRunner(userLogger, taskId, taskType, progress, node, clientMongoOperator) {

            @Override
            protected void initialCompare(@NonNull IPdkConnector sourceConnector, @NonNull IPdkConnector targetConnector, @NonNull IAutoCompare autoCompare) throws Exception {
                long beginTimes = System.currentTimeMillis();
                while (true) {
                    if (!isRunning()) return;
                    if (isInitialed.get()) break;

                    Thread.sleep(1000L);
                }
                userLogger.info("Wait initial {}ms", System.currentTimeMillis() - beginTimes);

                super.initialCompare(sourceConnector, targetConnector, autoCompare);
            }

            @Override
            protected void incrementalCompare(@NonNull IPdkConnector sourceConnector, @NonNull IPdkConnector targetConnector, @NonNull IAutoCompare autoCompare) throws Exception {
                List<TapEvent> tapEvents;
                while (isRunning()) {
                    do {
                        tapEvents = incrementQueue.poll(1, TimeUnit.SECONDS);
                        if (!isRunning()) return;

//                        //Output log every 30 seconds
//                        if (System.currentTimeMillis() / 1000 % 30 == 0) {
//                            logger.info("Increment event is empty");
//                        }
                    } while (null == tapEvents);

                    // update from observable-module
                    Optional.ofNullable(pipelineDelay.getDelay(taskId, dataNodeId)).ifPresent(processDelay::set);
                    Optional.ofNullable(pipelineDelay.getEventReferenceTime(taskId, dataNodeId)).ifPresent(lastReferenceTimes::set);

                    CompareTableItem tableItem;
                    CompareRecord sourceRecord;
                    for (TapEvent tapEvent : tapEvents) {
                        if (tapEvent instanceof TapRecordEvent) {
                            String tableName = ((TapRecordEvent) tapEvent).getTableId();
                            TapTable tapTable = sourceConnector.getTapTable(tableName);

                            // filter non-listening table
                            tableItem = progress.getTableItem(tableName);
                            if (null == tableItem) continue;

                            // Enter A when the synchronization task is normal, Otherwise use B or C whichever is smaller
                            // - A: referenceTimes < lastReferenceTimes
                            // - B: processDelay*1.5 - (SystemTimes - eventTimes)
                            // - C: 5 * 60 * 1000L
                            if (Optional.ofNullable(((TapRecordEvent) tapEvent).getReferenceTime()).map(referenceTimes -> {
                                if (referenceTimes <= lastReferenceTimes.get()) {
                                    return 0L;
                                } else {
                                    long delay = 0;
                                    if (null != tapEvent.getTime()) {
                                        delay = Math.max(0, (long) (processDelay.get() * 1.5) - (System.currentTimeMillis() - tapEvent.getTime()));
                                    }
                                    return Math.min(delay, 5 * 60 * 1000L);
                                }
                            }).map(delayTimes -> { // wait delay times
                                try {
                                    long interval = 200;
                                    while (delayTimes > 0) {
                                        Thread.sleep(Math.min(interval, delayTimes));
                                        if (!isRunning() || isStopping()) return true;
                                        delayTimes -= interval;
                                    }
                                    return false;
                                } catch (InterruptedException e) {
                                    return true;
                                }
                            }).orElse(false)) return; // exit in true

                            if (tapEvent instanceof TapUpdateRecordEvent) {
                                //update event
                                sourceRecord = toCompareRecord(sourceConnector.getConnId(), tapTable, ((TapUpdateRecordEvent) tapEvent).getAfter());
                            } else if (tapEvent instanceof TapInsertRecordEvent) {
                                //insert event
                                sourceRecord = toCompareRecord(sourceConnector.getConnId(), tapTable, ((TapInsertRecordEvent) tapEvent).getAfter());
                            } else if (tapEvent instanceof TapDeleteRecordEvent) {
                                //delete event
                                sourceRecord = toCompareRecord(sourceConnector.getConnId(), tapTable, ((TapDeleteRecordEvent) tapEvent).getBefore());
                            } else {
                                //no support events
                                continue;
                            }

                            LinkedHashMap<String, Object> keymap = new LinkedHashMap<>();
                            for (String k : sourceRecord.getKeyNames()) {
                                keymap.put(k, sourceRecord.getDataValue(k));
                            }

                            CompareRecord targetRecord = targetConnector.queryByKey(tableName, keymap, sourceRecord.getKeyNames());
                            if (tapEvent instanceof TapDeleteRecordEvent) {
                                if (null != targetRecord) {
                                    targetRecord.getOriginalKey().putAll(sourceRecord.getOriginalKey());
                                    //target has more data
                                }
                            } else if (null == targetRecord) {
                                //source has more data
                                autoCompare.autoCompare(TaskAutoInspectResultDto.parse(taskId, sourceRecord, targetConnector.getConnId(), tableName));
                            } else {
                                targetRecord.getOriginalKey().putAll(sourceRecord.getOriginalKey());
                                switch (sourceRecord.compare(targetRecord)) {
                                    case MoveTarget:
                                    case MoveSource:
                                        logger.info("Difference data keys '{}': {}, '{}': {}", sourceRecord.getTableName(), JSON.toJSONString(sourceRecord.getOriginalKey()), sourceRecord.getTableName(), JSON.toJSONString(targetRecord.getOriginalKey()));
                                        break;
                                    case Diff:
                                        autoCompare.autoCompare(TaskAutoInspectResultDto.parse(taskId, sourceRecord, targetRecord));
                                        break;
                                    case Ok:
                                        autoCompare.fix(TaskAutoInspectResultDto.parse(taskId, sourceRecord, targetRecord));
                                        break;
                                    default:
                                        break;
                                }
                            }
                        }
                    }
                }

            }

            @Override
            protected boolean isRunning() {
                return _thisNode.isRunning() && !Thread.interrupted();
            }

            @Override
            protected boolean isStopping() {
                return false;
            }

            @Override
            protected void errorHandle(Throwable e, String msg) {
                _thisNode.errorHandle(e, msg);
            }
        });
    }

    @Override
    protected void handleTapdataCompleteSnapshotEvent() {
        super.handleTapdataCompleteSnapshotEvent();
        isInitialed.set(true);
    }

    @Override
    void processEvents(List<TapEvent> tapEvents) {
        if (null == tapEvents) return;
        if (!isInitialed.get()) return;

        while (isRunning()) {
            try {
                if (incrementQueue.offer(tapEvents, 100, TimeUnit.MILLISECONDS)) {
                    break;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    void processShareLog(List<TapdataShareLogEvent> tapdataShareLogEvents) {
        throw new UnsupportedOperationException();
    }

    private CompareRecord toCompareRecord(ObjectId connectionId, TapTable tapTable, Map<String, Object> data) {
        LinkedHashMap<String, Object> keymap = new LinkedHashMap<>();
        for (String pk : tapTable.primaryKeys()) {
            keymap.put(pk, data.get(pk));
        }

        return new CompareRecord(tapTable.getName(), connectionId, keymap, new LinkedHashSet<>(keymap.keySet()), data);
    }

}
