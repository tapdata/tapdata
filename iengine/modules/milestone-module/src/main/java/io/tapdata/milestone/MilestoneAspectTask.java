package io.tapdata.milestone;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Element;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.ParentTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.*;
import io.tapdata.aspect.task.AbstractAspectTask;
import io.tapdata.aspect.task.AspectTaskSession;
import io.tapdata.aspect.taskmilestones.*;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.TapCodeException;
import io.tapdata.exception.TmUnavailableException;
import io.tapdata.milestone.constants.MilestoneStatus;
import io.tapdata.milestone.entity.MilestoneEntity;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.tapdata.aspect.TableInitFuncAspect.STATE_END;
import static io.tapdata.aspect.TableInitFuncAspect.STATE_START;
import static io.tapdata.aspect.TableInitFuncAspect.STATE_PROCESS;
import static io.tapdata.aspect.taskmilestones.EngineDeductionAspect.*;

/**
 * 设计要点：
 * <ul>
 *     <li>所有指标使用覆盖方式更新，体现当前任务运行状态</li>
 *     <li>任务重启，初始化不会进入</li>
 *     <li>任务重启，全量不一定进入</li>
 *     <li>全量读完，不代表同步完成</li>
 *     <li>全量迁移完成，不能以同步数据量一样来标识，中间数据可能被过滤</li>
 *     <li>增量任务，增量阶段不会完成</li>
 *     <li>增量开始，不是马上读到增量数据（Oracle 开 logminer 会很久）</li>
 *     <li>合并场景，源节点会有多个</li>
 * </ul>
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/2/14 15:54 Create
 */
@AspectTaskSession(includeTypes = {TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC, TaskDto.SYNC_TYPE_CONN_HEARTBEAT, TaskDto.SYNC_TYPE_LOG_COLLECTOR,TaskDto.SYNC_TYPE_MEM_CACHE})
public class MilestoneAspectTask extends AbstractAspectTask {

    protected static final String KPI_TASK = "TASK";
    protected static final String KPI_DATA_NODE_INIT = "DATA_NODE_INIT";
    protected static final String KPI_NODE = "NODE";
    protected static final String KPI_SNAPSHOT = "SNAPSHOT";
    protected static final String KPI_CDC = "CDC";
    protected static final String KPI_SNAPSHOT_READ = "SNAPSHOT_READ";
    protected static final String KPI_OPEN_CDC_READ = "OPEN_CDC_READ";
    protected static final String KPI_CDC_READ = "CDC_READ";
    protected static final String KPI_SNAPSHOT_WRITE = "SNAPSHOT_WRITE";
    protected static final String KPI_CDC_WRITE = "CDC_WRITE";
    protected static final String KPI_TABLE_INIT = "TABLE_INIT";
    protected static final String KPI_DEDUCTION = "DEDUCTION";

    private final Map<String, MilestoneEntity> milestones = new ConcurrentHashMap<>();
    private final Map<String, Map<String, MilestoneEntity>> nodeMilestones = new ConcurrentHashMap<>();

    private final ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);
    private ClientMongoOperator clientMongoOperator;
    private final Map<String, MilestoneStatus> dataNodeInitMap = new HashMap<>();
    private final Set<String> targetNodes = new HashSet<>();
	private final AtomicLong snapshotTableCounts = new AtomicLong(0);
	private final AtomicLong snapshotTableProgress = new AtomicLong(0);

    public MilestoneAspectTask() {
        init();
    }

    protected void init() {
        observerHandlers.register(PDKNodeInitAspect.class, this::handlePDKNodeInit);
        observerHandlers.register(DataNodeInitAspect.class, this::handleDataNodeInit);
        observerHandlers.register(DataNodeCloseAspect.class, this::handleDataNodeClose);
        observerHandlers.register(ProcessorNodeInitAspect.class, this::handleProcessNodeInit);
        observerHandlers.register(ProcessorNodeCloseAspect.class, this::handleProcessNodeClose);
        observerHandlers.register(TableInitFuncAspect.class, this::handleTableInit);
        observerHandlers.register(EngineDeductionAspect.class,this::handleEngineDeduction);
        observerHandlers.register(RetryLifeCycleAspect.class, this::handleRetry);
        nodeRegister(SnapshotReadBeginAspect.class, KPI_SNAPSHOT_READ, (aspect, m) -> {
            m.setProgress(0L);
            m.setTotals((long) aspect.getTables().size());
            setRunning(m);
            taskMilestone(KPI_SNAPSHOT, this::setRunning);
        });
        nodeRegister(SnapshotReadEndAspect.class, KPI_SNAPSHOT_READ, (aspect, m) -> setFinish(m));
        nodeRegister(SnapshotReadErrorAspect.class, KPI_SNAPSHOT_READ, (aspect, m) -> {
            setError(aspect, m);
            taskMilestone(KPI_SNAPSHOT, tm -> setError(aspect, tm));
        });
        nodeRegister(SnapshotReadTableEndAspect.class, KPI_SNAPSHOT_READ, (aspect, m) -> {
            m.addProgress(1);
            snapshotTableProgress.addAndGet(1);
            taskMilestone(KPI_SNAPSHOT, tm -> tm.setProgress(snapshotTableProgress.get()));
        });
        nodeRegister(Snapshot2CDCAspect.class, KPI_SNAPSHOT_READ, (aspect, m) -> {
            setFinish(m);
            taskMilestone(KPI_SNAPSHOT, this::setRunning); // fix status
        });
        nodeRegister(CDCReadBeginAspect.class, KPI_OPEN_CDC_READ, (aspect, m) -> {
            if (hasSnapshot()) {
                taskMilestone(KPI_SNAPSHOT, this::setFinish); // fix status
            }
            setRunning(m);
            taskMilestone(KPI_CDC, this::setRunning);
        });
        nodeRegister(CDCReadStartedAspect.class, (nodeId, aspect) -> {
            nodeMilestones(nodeId, KPI_OPEN_CDC_READ, this::setFinish);
            nodeMilestones(nodeId, KPI_CDC_READ, this::setRunning);
        });
        nodeRegister(CDCReadErrorAspect.class, (nodeId, aspect) -> nodeMilestones(nodeId, KPI_OPEN_CDC_READ, m -> {
            if (null == m.getEnd()) {
                setError(aspect, m);
            } else {
                nodeMilestones(nodeId, KPI_CDC_READ, m2 -> setError(aspect, m2));
            }
        }));
        nodeRegister(SnapshotWriteBeginAspect.class, KPI_SNAPSHOT_WRITE, (aspect, m) -> setRunning(m));
        nodeRegister(SnapshotWriteEndAspect.class, KPI_SNAPSHOT_WRITE, (aspect, m) -> {
            setFinish(m);
            if (snapshotTableProgress.get() >= snapshotTableCounts.get()) {
                taskMilestone(KPI_SNAPSHOT, this::setFinish);
            }
        });
        nodeRegister(CDCWriteBeginAspect.class, (nodeId, aspect) -> {
            if (hasSnapshot()) {
                nodeMilestones(nodeId, KPI_SNAPSHOT_WRITE, this::setFinish); // fix status
            }
            nodeMilestones(nodeId, KPI_CDC_WRITE, this::setRunning);
            taskMilestone(KPI_CDC, this::setFinish);
        });
        nodeRegister(WriteErrorAspect.class, (nodeId, aspect) -> nodeMilestones(
                nodeId, KPI_SNAPSHOT_WRITE, m -> {
                    if (null == m.getEnd()) {
                        setError(aspect, m);
                    } else {
                        nodeMilestones(nodeId, KPI_CDC_WRITE, m2 -> setError(aspect, m2));
                    }
        }));
    }

    @Override
    public void onStart(TaskStartAspect startAspect) {
        log.trace("Start task milestones: {}({})", task.getId().toHexString(), task.getName());
        taskMilestone(KPI_TASK, this::setFinish);
        taskMilestone(KPI_DEDUCTION,null);
        if (!TaskDto.SYNC_TYPE_LOG_COLLECTOR.equals(task.getSyncType())) {
            taskMilestone(KPI_TABLE_INIT, null);
        }
        taskMilestone(KPI_DATA_NODE_INIT, null);
        if (hasSnapshot()) taskMilestone(KPI_SNAPSHOT, null);
        if (hasCdc()) taskMilestone(KPI_CDC, null);

        for (Node<?> n : task.getDag().getNodes()) {
            if (n.isDataNode()) {
                dataNodeInitMap.put(n.getId(), MilestoneStatus.WAITING);
            }
        }

        this.clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
        executorService.scheduleWithFixedDelay(this::storeMilestone, 200, 5000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void onStop(TaskStopAspect stopAspect) {
        log.trace("Stop task milestones: {}({}) ", task.getId().toHexString(), task.getName());
        try {
            // Release resources
            executorService.shutdown();
        } finally {
            // store last status
            storeMilestone();
        }
    }

    protected boolean hasSnapshot() {
        return ParentTaskDto.TYPE_INITIAL_SYNC_CDC.equals(task.getType()) || ParentTaskDto.TYPE_INITIAL_SYNC.equals(task.getType());
    }

    protected boolean hasCdc() {
        return ParentTaskDto.TYPE_INITIAL_SYNC_CDC.equals(task.getType()) || ParentTaskDto.TYPE_CDC.equals(task.getType());
    }

    protected Void handleDataNodeInit(DataNodeInitAspect aspect) {
        DataProcessorContext dataProcessorContext = aspect.getDataProcessorContext();
        String nodeId = nodeId(dataProcessorContext);
        nodeMilestones(nodeId, KPI_NODE, this::setRunning);
        taskMilestone(KPI_DATA_NODE_INIT, this::setRunning);
        Node<?> node = dataProcessorContext.getNode();
        List<? extends Node<?>> predecessors = node.predecessors();
        if (null == predecessors || predecessors.isEmpty()) {
            if (node instanceof TableNode && !node.disabledNode()) {
                snapshotTableCounts.addAndGet(1);
            } else if (node instanceof DatabaseNode && !node.disabledNode()) {
                DatabaseNode databaseNode = (DatabaseNode) node;
                snapshotTableCounts.addAndGet(databaseNode.tableSize());
            }
            taskMilestone(KPI_SNAPSHOT, tm -> tm.setTotals(snapshotTableCounts.get()));
        }
        dataNodeInitMap.computeIfPresent(nodeId, (k, v) -> MilestoneStatus.RUNNING);
        return null;
    }

    protected Void handleDataNodeClose(DataNodeCloseAspect aspect) {
        String nodeId = nodeId(aspect.getDataProcessorContext());
        nodeMilestones(nodeId, KPI_NODE, this::setFinish);
        dataNodeInitMap.computeIfPresent(nodeId, (k, v) -> MilestoneStatus.FINISH);
        return null;
    }

    protected Void handleProcessNodeInit(ProcessorNodeInitAspect aspect) {
        String nodeId = nodeId(aspect.getProcessorBaseContext());
        nodeMilestones(nodeId, KPI_NODE, this::setRunning);
        taskMilestone(KPI_DATA_NODE_INIT, this::setRunning);
        return null;
    }

    protected Void handleProcessNodeClose(ProcessorNodeCloseAspect aspect) {
        String nodeId = nodeId(aspect.getProcessorBaseContext());
        nodeMilestones(nodeId, KPI_NODE, this::setFinish);
        return null;
    }

    protected Void handlePDKNodeInit(PDKNodeInitAspect aspect) {
        String nodeId = nodeId(aspect.getDataProcessorContext());
        nodeMilestones(nodeId, KPI_NODE, this::setFinish);
        return null;
    }

    protected synchronized Void handleTableInit(TableInitFuncAspect aspect) {
        String nodeId = nodeId(aspect.getDataProcessorContext());
        switch (aspect.getState()) {
            case STATE_START:
                taskMilestone(KPI_TABLE_INIT, this::setRunning);
                targetNodes.add(nodeId);
                nodeMilestones(nodeId, KPI_TABLE_INIT, milestone -> {
                    milestone.setProgress(0L);
                    milestone.setTotals(aspect.getTotals());
                    setRunning(milestone);
                });
                break;
            case STATE_PROCESS:
                taskMilestone(KPI_TABLE_INIT, this::setRunning);
                nodeMilestones(nodeId, KPI_TABLE_INIT, milestone -> {
                    milestone.setProgress(aspect.getCompletedCounts());
                    milestone.setTotals(aspect.getTotals());
                });
                break;
            case STATE_END:
                taskMilestone(KPI_TABLE_INIT, this::setFinish);
                Throwable error = aspect.getThrowable();
                if (null == error) {
                    nodeMilestones(nodeId, KPI_TABLE_INIT, milestone -> {
                        milestone.setProgress(aspect.getTotals());
                        milestone.setTotals(aspect.getTotals());
                        setFinish(milestone);
                    });
                } else {
                    nodeMilestones(nodeId, KPI_TABLE_INIT, getErrorConsumer(error.getMessage()));
                }
                break;
            default:
                break;
        }
        return null;
    }

    protected Void handleEngineDeduction(EngineDeductionAspect aspect) {
        switch (aspect.getState()){
            case DEDUCTION_START:
                taskMilestone(KPI_DEDUCTION, this::setRunning);
                break;
            case DEDUCTION_END:
                taskMilestone(KPI_DEDUCTION, this::setFinish);
                break;
            case DEDUCTION_ERROR:
                taskMilestone(KPI_DEDUCTION, (m)->{setError(aspect,m);});
                break;
            default:
                break;
        }
        return null;
    }

    protected Void handleRetry(RetryLifeCycleAspect aspect) {
//        log.info("Handle retry data, retry op: {}, retrying: {}, start ts: {}, retry times: {}/{}, success: {}",
//                aspect.getRetryOp(),
//                aspect.isRetrying(),
//                aspect.getStartRetryTs(),
//                aspect.getRetryTimes(),
//                aspect.getTotalRetries(),
//                aspect.getSuccess());
        // 1. get running milestone
        MilestoneEntity runningMilestone = milestones.values().stream().filter(m -> m.getStatus() == MilestoneStatus.RUNNING).findFirst().orElse(null);
        // 2. get cdc milestone when all finish
        if (runningMilestone == null) {
            runningMilestone = milestones.values().stream().filter(m -> KPI_CDC.equals(m.getCode())).findFirst().orElse(null);
        }
        // 3. update retry status to milestone
        Optional.ofNullable(runningMilestone).ifPresent(milestoneEntity -> {
            milestoneEntity.setRetrying(aspect.isRetrying());
            milestoneEntity.setRetryTimes(aspect.getRetryTimes());
            milestoneEntity.setStartRetryTs(aspect.getStartRetryTs());
            milestoneEntity.setEndRetryTs(aspect.getEndRetryTs());
            milestoneEntity.setNextRetryTs(aspect.getNextRetryTs());
            milestoneEntity.setTotalOfRetries(aspect.getTotalRetries());
            milestoneEntity.setRetryOp(aspect.getRetryOp());
            milestoneEntity.setRetrySuccess(aspect.getSuccess());
            milestoneEntity.setRetryMetadata(aspect.getRetryMetadata());
        });
        return null;
    }

    protected void storeMilestoneWhenTargetNodeTableInit(MilestoneEntity milestone, String nid, AtomicLong totals, AtomicLong completed) {
        nodeMilestones(nid, KPI_TABLE_INIT, m -> {
            milestone.setBegin(Math.min(milestone.getBegin(), m.getBegin()));
            if (null == m.getEnd()) {
                milestone.setEnd(null);
            } else if (null != milestone.getEnd()) {
                milestone.setEnd(Math.max(milestone.getEnd(), m.getEnd()));
            }

            milestone.setTotals(totals.addAndGet(m.getTotals()));
            milestone.setProgress(completed.addAndGet(m.getProgress()));
            if (null != m.getErrorMessage()) {
                milestone.setStatus(MilestoneStatus.ERROR);
                milestone.setErrorMessage(m.getErrorMessage());
            }
        });
    }

    protected void storeMilestoneWhenSyncTypeIsLogCollector() {
        taskMilestone(KPI_TABLE_INIT, milestone -> {
            milestone.setStatus(MilestoneStatus.WAITING);
            if (targetNodes.isEmpty()) return;

            milestone.setBegin(System.currentTimeMillis());
            milestone.setEnd(0L);
            milestone.setStatus(MilestoneStatus.RUNNING);
            AtomicLong totals = new AtomicLong(0);
            AtomicLong completed = new AtomicLong(0);
            for (String nid : targetNodes) {
                storeMilestoneWhenTargetNodeTableInit(milestone, nid, totals, completed);
            }
            if (MilestoneStatus.ERROR != milestone.getStatus() && totals.get() == completed.get()) {
                milestone.setStatus(MilestoneStatus.FINISH);
            }
        });
    }

    protected void storeMilestoneDataNodeInit(MilestoneEntity m) {
        if (MilestoneStatus.FINISH == m.getStatus()) return; // return if finish
        long progress = 0;
        long totals = dataNodeInitMap.size();
        for (MilestoneStatus status : dataNodeInitMap.values()) {
            if (MilestoneStatus.WAITING != status) {
                progress++;
            }
        }

        if (0 == totals || progress == totals) {
            setFinish(m);
        } else {
            if (null == m.getBegin()) {
                // update first time
                m.setBegin(System.currentTimeMillis());
            }
            m.setStatus(0 == progress ? MilestoneStatus.WAITING : MilestoneStatus.RUNNING);
        }
        m.setTotals(totals);
        m.setProgress(progress);
    }

    protected void storeMilestone() {
        try {
            // set task table init
            if (!TaskDto.SYNC_TYPE_LOG_COLLECTOR.equals(task.getSyncType())) {
                storeMilestoneWhenSyncTypeIsLogCollector();
            }
            taskMilestone(KPI_DATA_NODE_INIT, this::storeMilestoneDataNodeInit);
            synchronized (milestones) {
                clientMongoOperator.update(
                        Query.query(Criteria.where("_id").is(task.getId()))
                        , Update.update("attrs.milestone", milestones).set("attrs.nodeMilestones", nodeMilestones)
                                .set("syncStatus", getTaskSyncStatus())
                        , ConnectorConstant.TASK_COLLECTION);
            }
        } catch (Exception e) {
            if (TmUnavailableException.notInstance(e)) {
                log.warn("Save milestone failed: {}", e.getMessage(), e);
            }
        }
    }

    protected String getTaskSyncStatus() {
        Collection<MilestoneEntity> values = new ArrayList<>(milestones.values());
        List<MilestoneEntity> sorted = values.stream()
                .filter(m -> Objects.nonNull(m) && !MilestoneStatus.WAITING.equals(m.getStatus()))
                .sorted((m1, m2) -> {
                    MilestoneStatus s1 = m1.getStatus();
                    MilestoneStatus s2 = m2.getStatus();
                    if (KPI_CDC.equals(m1.getCode())) return -1;
                    if (KPI_CDC.equals(m2.getCode())) return 1;
                    if (MilestoneStatus.RUNNING.equals(s1)) return -1;
                    if (MilestoneStatus.RUNNING.equals(s2)) return 1;
                    Long e1 = m1.getEnd();
                    Long e2 = m2.getEnd();
                    if (null == e1) e1 = 0L;
                    if (null == e2) e2 = 0L;
                    return e2.intValue() - e1.intValue();
                }).collect(Collectors.toList());
        if (sorted.isEmpty()) return KPI_TASK;
        return sorted.get(0).getCode();
    }

    protected void taskMilestone(String code, Consumer<MilestoneEntity> consumer) {
        MilestoneEntity entity = milestones.get(code);
        if (null == entity) {
            synchronized (milestones) {
                entity = milestones.computeIfAbsent(code, s -> new MilestoneEntity(code, MilestoneStatus.WAITING));
            }
        }
        if (null != consumer) {
            consumer.accept(entity);
        }
    }

    protected void nodeMilestones(String nodeId, String code, Consumer<MilestoneEntity> consumer) {
        Map<String, MilestoneEntity> nodeMap = nodeMilestones.get(nodeId);
        if (null == nodeMap) {
            synchronized (nodeMilestones) {
                nodeMap = nodeMilestones.computeIfAbsent(nodeId, s -> new HashMap<>());
            }
        }

        MilestoneEntity entity = nodeMap.computeIfAbsent(code, s -> new MilestoneEntity(code, MilestoneStatus.WAITING));
        consumer.accept(entity);
    }

    protected <T extends DataNodeAspect<T>> void nodeRegister(Class<T> clz, BiConsumer<String, T> consumer) {
        observerHandlers.register(clz, aspect -> {
            String nodeId = nodeId(aspect.getDataProcessorContext());
            consumer.accept(nodeId, aspect);
            return null;
        });
    }

    protected  <T extends DataNodeAspect<T>> void nodeRegister(Class<T> clz, String code, BiConsumer<T, MilestoneEntity> consumer) {
        observerHandlers.register(clz, aspect -> {
            String nodeId = nodeId(aspect.getDataProcessorContext());
            nodeMilestones(nodeId, code, m -> consumer.accept(aspect, m));
            return null;
        });
    }

    protected String nodeId(ProcessorBaseContext context) {
        return Optional.ofNullable(context)
                .map(ProcessorBaseContext::getNode)
                .map(Element::getId)
                .orElse(null);
    }

    protected void setRunning(MilestoneEntity milestone) {
        if (null == milestone.getBegin()) {
            milestone.setBegin(System.currentTimeMillis());
            milestone.setStatus(MilestoneStatus.RUNNING);
        }
        milestone.setEnd(null);
        milestone.setErrorMessage(null);
    }

    protected void setFinish(MilestoneEntity milestone) {
        if (null == milestone.getBegin()) {
            milestone.setBegin(System.currentTimeMillis());
        }
        if (null == milestone.getEnd()) {
            milestone.setEnd(System.currentTimeMillis());
            milestone.setStatus(MilestoneStatus.FINISH);
        }
    }

    protected <T extends AbsDataNodeErrorAspect<T>> void setError(T aspect, MilestoneEntity m) {
        setError(aspect.getError(), m);
    }
    protected <T extends EngineDeductionAspect> void setError(T aspect, MilestoneEntity m) {
        setError(aspect.getError(), m);
    }

    protected void setError(Throwable error, MilestoneEntity m) {
        m.setEnd(System.currentTimeMillis());
        m.setStatus(MilestoneStatus.ERROR);
        m.setErrorMessage(Optional.ofNullable(error).map(Throwable::getMessage).orElse(null));
        if (error != null && m.getErrorCode() == null) {
            m.setErrorCode(Optional.ofNullable(CommonUtils.describeErrorCode(error)).orElse(TaskProcessorExCode_11.UNKNOWN_ERROR));
        }
        if (error != null && m.getStackMessage() == null) {
            m.setStackMessage(ExceptionUtils.getStackTrace(error));
        }
        Throwable tapCodeError = CommonUtils.matchThrowable(error, TapCodeException.class);
        if (tapCodeError != null) {
            m.setDynamicDescriptionParameters(((TapCodeException) tapCodeError).getDynamicDescriptionParameters());
        }
    }

    protected Consumer<MilestoneEntity> getErrorConsumer(String errorMessage) {
        return m -> {
            m.setEnd(System.currentTimeMillis());
            m.setStatus(MilestoneStatus.ERROR);
            m.setErrorMessage(errorMessage);
        };
    }

}
