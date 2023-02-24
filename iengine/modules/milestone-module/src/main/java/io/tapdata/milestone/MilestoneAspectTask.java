package io.tapdata.milestone;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Element;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.*;
import io.tapdata.aspect.task.AbstractAspectTask;
import io.tapdata.aspect.task.AspectTaskSession;
import io.tapdata.milestone.constants.MilestoneStatus;
import io.tapdata.milestone.entity.MilestoneEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/2/14 15:54 Create
 */
@AspectTaskSession(includeTypes = {TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC})
public class MilestoneAspectTask extends AbstractAspectTask {
    private final static Logger logger = LogManager.getLogger(MilestoneAspectTask.class);

    private final static String KPI_TASK = "TASK";
    private final static String KPI_DATA_NODE_INIT = "DATA_NODE_INIT";
    private final static String KPI_NODE = "NODE";
    private final static String KPI_BATCH_READ = "BATCH_READ";
    private final static String KPI_STREAM_READ = "STREAM_READ";
    private final static String KPI_OPEN_STREAM_READ = "OPEN_STREAM_READ";
    private final static String KPI_WRITE_RECORD = "WRITE_RECORD";
    private final static String KPI_TABLE_INIT = "TABLE_INIT";

    private final Map<String, MilestoneEntity> milestones = new ConcurrentHashMap<>();
    private final Map<String, Map<String, MilestoneEntity>> nodeMilestones = new ConcurrentHashMap<>();

    private final ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);
    private ClientMongoOperator clientMongoOperator;
    private ObjectId taskId;
    private final Map<String, MilestoneStatus> dataNodeInitMap = new HashMap<>();
    private final AtomicInteger tableCounts = new AtomicInteger(0);
    private final Map<String, AtomicInteger> tableInits = new ConcurrentHashMap<>();

    public MilestoneAspectTask() {
        observerHandlers.register(PDKNodeInitAspect.class, this::handlePDKNodeInit);
        observerHandlers.register(DataNodeInitAspect.class, this::handleDataNodeInit);
        observerHandlers.register(DataNodeCloseAspect.class, this::handleDataNodeClose);
        observerHandlers.register(ProcessorNodeInitAspect.class, this::handleProcessNodeInit);
        observerHandlers.register(ProcessorNodeCloseAspect.class, this::handleProcessNodeClose);

        observerHandlers.register(BatchReadFuncAspect.class, this::handleBatchRead);
        observerHandlers.register(StreamReadFuncAspect.class, this::handleStreamRead);
        observerHandlers.register(WriteRecordFuncAspect.class, this::handleWriteRecord);
        observerHandlers.register(CreateTableFuncAspect.class, this::handleCreateTable);
    }

    @Override
    public void onStart(TaskStartAspect startAspect) {
        taskId = task.getId();
        logger.info("Start task milestones: {}({})", taskId.toHexString(), task.getName());
        taskMilestone(KPI_TASK, this::setFinish);

        for (Node<?> n : startAspect.getTask().getDag().getNodes()) {
            if (n.isDataNode()) {
                dataNodeInitMap.put(n.getId(), MilestoneStatus.WAITING);
            }
        }

        this.clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
        executorService.scheduleWithFixedDelay(this::storeMilestone, 200, 5000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void onStop(TaskStopAspect stopAspect) {
        logger.info("Stop task milestones: {}({}) ", taskId.toHexString(), task.getName());
        try {
            // Release resources
            executorService.shutdown();
        } finally {
            // store last status
            storeMilestone();
        }
    }

    private Void handleDataNodeInit(DataNodeInitAspect aspect) {
        DataProcessorContext dataProcessorContext = aspect.getDataProcessorContext();
        String nodeId = nodeId(dataProcessorContext);
        nodeMilestones(nodeId, KPI_NODE, this::setRunning);
        dataNodeInitMap.computeIfPresent(nodeId, (k, v) -> MilestoneStatus.RUNNING);

        // if target node
        if (null != dataProcessorContext.getTargetConn()) {
            tableInits.computeIfAbsent(nodeId, s -> new AtomicInteger(0));

            Optional.ofNullable(dataProcessorContext.getTapTableMap()).map(tableMap -> {
                int size = tableMap.size();

                tableCounts.addAndGet(size);
                taskMilestone(KPI_TABLE_INIT, (milestone) -> {
                    milestone.setTotals(tableCounts.get());
                    milestone.setProgress(0);
                });

                nodeMilestones(nodeId, KPI_TABLE_INIT, (milestone) -> {
                    setRunning(milestone);
                    milestone.setTotals(size);
                    milestone.setProgress(0);
                });

                return tableMap;
            });
        }

        return null;
    }

    private Void handleDataNodeClose(DataNodeCloseAspect aspect) {
        String nodeId = nodeId(aspect.getDataProcessorContext());
        nodeMilestones(nodeId, KPI_NODE, this::setFinish);
        dataNodeInitMap.computeIfPresent(nodeId, (k, v) -> MilestoneStatus.FINISH);
        return null;
    }

    private Void handleProcessNodeInit(ProcessorNodeInitAspect aspect) {
        String nodeId = nodeId(aspect.getProcessorBaseContext());
        nodeMilestones(nodeId, KPI_NODE, this::setRunning);
        return null;
    }

    private Void handleProcessNodeClose(ProcessorNodeCloseAspect aspect) {
        String nodeId = nodeId(aspect.getProcessorBaseContext());
        nodeMilestones(nodeId, KPI_NODE, this::setFinish);
        return null;
    }

    private Void handlePDKNodeInit(PDKNodeInitAspect aspect) {
        String nodeId = nodeId(aspect.getDataProcessorContext());
        nodeMilestones(nodeId, KPI_NODE, this::setFinish);
        return null;
    }

    private Void handleBatchRead(BatchReadFuncAspect aspect) {
        String nodeId = nodeId(aspect.getDataProcessorContext());
        switch (aspect.getState()) {
            case BatchReadFuncAspect.STATE_START:
                nodeMilestones(nodeId, KPI_BATCH_READ, this::setRunning);
                break;
            case BatchReadFuncAspect.STATE_END: {
                Throwable error = aspect.getThrowable();
                if (null == error) {
                    nodeMilestones(nodeId, KPI_BATCH_READ, this::setFinish);
                } else {
                    nodeMilestones(nodeId, KPI_BATCH_READ, getErrorConsumer(error.getMessage()));
                }
                break;
            }
            default:
                break;
        }
        return null;
    }

    private Void handleStreamRead(StreamReadFuncAspect aspect) {
        String nodeId = nodeId(aspect.getDataProcessorContext());
        switch (aspect.getState()) {
            case StreamReadFuncAspect.STATE_START:
                taskMilestone(KPI_STREAM_READ, this::setRunning);
                nodeMilestones(nodeId, KPI_OPEN_STREAM_READ, this::setRunning);
                break;
            case StreamReadFuncAspect.STATE_STREAM_STARTED: // has stream data
                nodeMilestones(nodeId, KPI_OPEN_STREAM_READ, this::setFinish);
                nodeMilestones(nodeId, KPI_STREAM_READ, this::setRunning);
                break;
            case StreamReadFuncAspect.STATE_END: {
                nodeMilestones.computeIfPresent(nodeId, (nid, nodeMap) -> {
                    Throwable error = aspect.getThrowable();
                    if (null == error) {
                        taskMilestone(KPI_STREAM_READ, this::setFinish);
                        nodeMap.computeIfPresent(KPI_STREAM_READ, (k, v) -> {
                            setFinish(v);
                            return v;
                        });
                        nodeMilestones(nodeId, KPI_OPEN_STREAM_READ, (m) -> {
                            if (MilestoneStatus.FINISH != m.getStatus()) {
                                setFinish(m);
                            }
                        });
                    } else {
                        taskMilestone(KPI_STREAM_READ, getErrorConsumer(error.getMessage()));
                        MilestoneEntity m = Optional.ofNullable(nodeMap.get(KPI_STREAM_READ)).orElse(nodeMap.get(KPI_OPEN_STREAM_READ));
                        if (null != m) {
                            m.setEnd(System.currentTimeMillis());
                            m.setStatus(MilestoneStatus.ERROR);
                            m.setErrorMessage(error.getMessage());
                        }
                    }
                    return nodeMap;
                });
                break;
            }
            default:
                break;
        }
        return null;
    }

    private Void handleWriteRecord(WriteRecordFuncAspect aspect) {
        String nodeId = nodeId(aspect.getDataProcessorContext());
        switch (aspect.getState()) {
            case WriteRecordFuncAspect.STATE_START:
                taskMilestone(KPI_WRITE_RECORD, this::setRunning);
                nodeMilestones(nodeId, KPI_WRITE_RECORD, this::setRunning);
                taskMilestone(KPI_TABLE_INIT, milestone -> {
                    milestone.setProgress(milestone.getTotals());
                    setFinish(milestone);
                });
                nodeMilestones(nodeId, KPI_TABLE_INIT, milestone -> {
                    milestone.setProgress(milestone.getTotals());
                    setFinish(milestone);
                });
                break;
            case WriteRecordFuncAspect.STATE_END: {
                Throwable error = aspect.getThrowable();
                if (null == error) {
                    // WRITE_RECORD can not be set to finnish.
//                    taskMilestone(KPI_WRITE_RECORD, this::setFinish);
//                    nodeMilestones(nodeId, KPI_WRITE_RECORD, this::setFinish);
                } else {
                    taskMilestone(KPI_WRITE_RECORD, getErrorConsumer(error.getMessage()));
                    nodeMilestones(nodeId, KPI_WRITE_RECORD, getErrorConsumer(error.getMessage()));
                }
                break;
            }
            default:
                break;
        }
        return null;
    }

    private Void handleCreateTable(CreateTableFuncAspect aspect) {
        String nodeId = nodeId(aspect.getDataProcessorContext());
        tableInits.computeIfPresent(nodeId, (k, v) -> {
            v.addAndGet(1);
            nodeMilestones(nodeId, KPI_TABLE_INIT, (milestone) -> {
                if (MilestoneStatus.FINISH == milestone.getStatus()
                        || null == milestone.getTotals()
                        || null == milestone.getProgress()) {
                    return;
                }

                milestone.setProgress(v.get());

                if (milestone.getTotals() > v.get()) {
                    setRunning(milestone);
                } else {
                    setFinish(milestone);
                }
            });
            return v;
        });
        taskMilestone(KPI_TABLE_INIT, (milestone) -> {
            if (MilestoneStatus.FINISH == milestone.getStatus()) return;

            int progress = 0;
            for (AtomicInteger i : tableInits.values()) {
                progress += i.get();
            }

            setRunning(milestone);
            milestone.setProgress(progress);
            if (tableCounts.get() <= progress) {
                setFinish(milestone);
            }
        });
        return null;
    }

    private void storeMilestone() {
        try {
            taskMilestone(KPI_DATA_NODE_INIT, m -> {
                if (MilestoneStatus.FINISH == m.getStatus()) return; // return if finish

                int progress = 0;
                int totals = dataNodeInitMap.size();
                for (MilestoneStatus status : dataNodeInitMap.values()) {
                    if (MilestoneStatus.WAITING != status) {
                        progress++;
                    }
                }

                if (0 == totals || progress == totals) {
                    setFinish(m);
                } else {
                    if (null == m.getBegin()) { // update first time
                        m.setBegin(System.currentTimeMillis());
                    }
                    m.setStatus(0 == progress ? MilestoneStatus.WAITING : MilestoneStatus.RUNNING);
                }
                m.setTotals(totals);
                m.setProgress(progress);
            });

            clientMongoOperator.update(
                    Query.query(Criteria.where("_id").is(taskId))
                    , Update.update("attrs.milestone", milestones).set("attrs.nodeMilestones", nodeMilestones)
                    , ConnectorConstant.TASK_COLLECTION);
        } catch (Exception e) {
            logger.warn("Save milestone failed: {}", e.getMessage(), e);
        }
    }

    private void taskMilestone(String code, Consumer<MilestoneEntity> consumer) {
        MilestoneEntity entity = milestones.get(code);
        if (null == entity) {
            synchronized (milestones) {
                entity = milestones.computeIfAbsent(code, s -> new MilestoneEntity(code, MilestoneStatus.WAITING));
            }
        }
        consumer.accept(entity);
    }

    private void nodeMilestones(String nodeId, String code, Consumer<MilestoneEntity> consumer) {
        Map<String, MilestoneEntity> nodeMap = nodeMilestones.get(nodeId);
        if (null == nodeMap) {
            synchronized (nodeMilestones) {
                nodeMap = nodeMilestones.computeIfAbsent(nodeId, s -> new HashMap<>());
            }
        }

        MilestoneEntity entity = nodeMap.computeIfAbsent(code, s -> new MilestoneEntity(code, MilestoneStatus.WAITING));
        consumer.accept(entity);
    }

    private String nodeId(ProcessorBaseContext context) {
        return Optional.ofNullable(context)
                .map(ProcessorBaseContext::getNode)
                .map(Element::getId)
                .orElse(null);
    }

    private void setRunning(MilestoneEntity milestone) {
        if (null == milestone.getBegin()) {
            milestone.setBegin(System.currentTimeMillis());
        }
        milestone.setEnd(null);
        milestone.setErrorMessage(null);
        milestone.setStatus(MilestoneStatus.RUNNING);
    }

    private void setFinish(MilestoneEntity milestone) {
        milestone.setEnd(System.currentTimeMillis());
        milestone.setStatus(MilestoneStatus.FINISH);
    }

    private Consumer<MilestoneEntity> getErrorConsumer(String errorMessage) {
        return (m) -> {
            m.setEnd(System.currentTimeMillis());
            m.setStatus(MilestoneStatus.ERROR);
            m.setErrorMessage(errorMessage);
        };
    }

}
