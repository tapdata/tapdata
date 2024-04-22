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
import io.tapdata.exception.TmUnavailableException;
import io.tapdata.milestone.constants.MilestoneStatus;
import io.tapdata.milestone.constants.SyncStatus;
import io.tapdata.milestone.entity.MilestoneEntity;
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

    protected final static String KPI_TASK = "TASK";
    protected final static String KPI_DATA_NODE_INIT = "DATA_NODE_INIT";
    protected final static String KPI_NODE = "NODE";
    protected final static String KPI_SNAPSHOT = "SNAPSHOT";
    protected final static String KPI_CDC = "CDC";
    protected final static String KPI_SNAPSHOT_READ = "SNAPSHOT_READ";
    protected final static String KPI_OPEN_CDC_READ = "OPEN_CDC_READ";
    protected final static String KPI_CDC_READ = "CDC_READ";
    protected final static String KPI_SNAPSHOT_WRITE = "SNAPSHOT_WRITE";
    protected final static String KPI_CDC_WRITE = "CDC_WRITE";
    protected final static String KPI_TABLE_INIT = "TABLE_INIT";

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

        nodeRegister(SnapshotReadBeginAspect.class, KPI_SNAPSHOT_READ, (aspect, m) -> {
            m.setProgress(0L);
            m.setTotals((long) aspect.getTables().size());
            setRunning(m);
            task.setSyncStatus(SyncStatus.SNAPSHOT_INIT.getType());
            taskMilestone(KPI_SNAPSHOT, this::setRunning);
        });
        nodeRegister(SnapshotReadEndAspect.class, KPI_SNAPSHOT_READ, (aspect, m) -> {
            setFinish(m);
        });
        nodeRegister(SnapshotReadErrorAspect.class, KPI_SNAPSHOT_READ, (aspect, m) -> {
            setError(aspect, m);
            taskMilestone(KPI_SNAPSHOT, (tm) -> setError(aspect, tm));
            task.setSyncStatus(SyncStatus.SNAPSHOT_FAILED.getType());
        });
        nodeRegister(SnapshotReadTableEndAspect.class, KPI_SNAPSHOT_READ, (aspect, m) -> {
            m.addProgress(1);
            snapshotTableProgress.addAndGet(1);
            taskMilestone(KPI_SNAPSHOT, (tm) -> {
                tm.setProgress(snapshotTableProgress.get());
            });
            task.setSyncStatus(SyncStatus.TABLE_INIT_COMPLETE.getType());
        });
        nodeRegister(Snapshot2CDCAspect.class, KPI_SNAPSHOT_READ, (aspect, m) -> {
            setFinish(m);
            taskMilestone(KPI_SNAPSHOT, this::setRunning); // fix status
            task.setSyncStatus(SyncStatus.SNAPSHOT_COMPLETED.getType());
        });
        nodeRegister(CDCReadBeginAspect.class, KPI_OPEN_CDC_READ, (aspect, m) -> {
            if (hasSnapshot()) {
                taskMilestone(KPI_SNAPSHOT, this::setFinish); // fix status
            }
            setRunning(m);
            taskMilestone(KPI_CDC, this::setRunning);
            task.setSyncStatus(SyncStatus.CDC_INIT.getType());
        });
        nodeRegister(CDCReadStartedAspect.class, (nodeId, aspect) -> {
            nodeKpi(nodeId, KPI_OPEN_CDC_READ, this::setFinish);
            nodeKpi(nodeId, KPI_CDC_READ, this::setRunning);
            task.setSyncStatus(SyncStatus.DO_CDC.getType());
        });
        nodeRegister(CDCReadErrorAspect.class, (nodeId, aspect) -> {
            nodeKpi(nodeId, KPI_OPEN_CDC_READ, (m) -> {
                if (null == m.getEnd()) {
                    setError(aspect, m);
                } else {
                    nodeKpi(nodeId, KPI_CDC_READ, (m2) -> setError(aspect, m2));
                }
            });
            task.setSyncStatus(SyncStatus.CDC_FAILED.getType());
        });
//        nodeRegister(CDCReadEndAspect.class, KPI_STREAM_READ, (aspect, m) -> setFinish(m));
        nodeRegister(SnapshotWriteBeginAspect.class, KPI_SNAPSHOT_WRITE, (aspect, m) -> {
            setRunning(m);
            task.setSyncStatus(SyncStatus.DO_SNAPSHOT.getType());
        });
        nodeRegister(SnapshotWriteEndAspect.class, KPI_SNAPSHOT_WRITE, (aspect, m) -> {
            setFinish(m);
            if (snapshotTableProgress.get() >= snapshotTableCounts.get()) {
                taskMilestone(KPI_SNAPSHOT, this::setFinish);
                task.setSyncStatus(SyncStatus.SNAPSHOT_COMPLETED.getType());
            }
        });
        nodeRegister(CDCWriteBeginAspect.class, (nodeId, aspect) -> {
            if (hasSnapshot()) {
                nodeKpi(nodeId, KPI_SNAPSHOT_WRITE, this::setFinish); // fix status
            }
            nodeKpi(nodeId, KPI_CDC_WRITE, this::setRunning);
            taskMilestone(KPI_CDC, this::setFinish);
            task.setSyncStatus(SyncStatus.DO_CDC.getType());
        });
        nodeRegister(WriteErrorAspect.class, (nodeId, aspect) -> {
            nodeKpi(nodeId, KPI_SNAPSHOT_WRITE, m -> {
                if (null == m.getEnd()) {
                    setError(aspect, m);
                } else {
                    nodeKpi(nodeId, KPI_CDC_WRITE, m2 -> setError(aspect, m2));
                }
            });
        });
    }

    @Override
    public void onStart(TaskStartAspect startAspect) {
        log.info("Start task milestones: {}({})", task.getId().toHexString(), task.getName());
        task.setSyncStatus(SyncStatus.TABLE_INIT_COMPLETE.getType());
        taskMilestone(KPI_TASK, this::setFinish);
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
        task.setSyncStatus(SyncStatus.NORMAL.getType());
        log.info("Stop task milestones: {}({}) ", task.getId().toHexString(), task.getName());
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
        task.setSyncStatus(SyncStatus.DATA_NODE_INIT.getType());
        DataProcessorContext dataProcessorContext = aspect.getDataProcessorContext();
        String nodeId = nodeId(dataProcessorContext);
        nodeMilestones(nodeId, KPI_NODE, this::setRunning);
        Node<?> node = dataProcessorContext.getNode();
        List<? extends Node<?>> predecessors = node.predecessors();
        if (null == predecessors || predecessors.isEmpty()) {
            if (node instanceof TableNode) {
                snapshotTableCounts.addAndGet(1);
            } else if (node instanceof DatabaseNode) {
                DatabaseNode databaseNode = (DatabaseNode) node;
                snapshotTableCounts.addAndGet(databaseNode.tableSize());
            }
            taskMilestone(KPI_SNAPSHOT, (tm) -> {
                tm.setTotals(snapshotTableCounts.get());
            });
        }
        dataNodeInitMap.computeIfPresent(nodeId, (k, v) -> MilestoneStatus.RUNNING);
        return null;
    }

    protected Void handleDataNodeClose(DataNodeCloseAspect aspect) {
        String nodeId = nodeId(aspect.getDataProcessorContext());
        nodeMilestones(nodeId, KPI_NODE, this::setFinish);
        dataNodeInitMap.computeIfPresent(nodeId, (k, v) -> MilestoneStatus.FINISH);
        task.setSyncStatus(SyncStatus.DATA_NODE_INIT_COMPLETED.getType());
        return null;
    }

    protected Void handleProcessNodeInit(ProcessorNodeInitAspect aspect) {
        task.setSyncStatus(SyncStatus.PROCESS_NODE_INIT.getType());
        String nodeId = nodeId(aspect.getProcessorBaseContext());
        nodeMilestones(nodeId, KPI_NODE, this::setRunning);
        return null;
    }

    protected Void handleProcessNodeClose(ProcessorNodeCloseAspect aspect) {
        task.setSyncStatus(SyncStatus.PROCESS_NODE_INIT_COMPLETED.getType());
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
            case TableInitFuncAspect.STATE_START:
                task.setSyncStatus(SyncStatus.TABLE_INIT_START.getType());
                targetNodes.add(nodeId);
                nodeMilestones(nodeId, KPI_TABLE_INIT, (milestone) -> {
                    milestone.setProgress(0L);
                    milestone.setTotals(aspect.getTotals());
                    setRunning(milestone);
                });
                break;
            case TableInitFuncAspect.STATE_PROCESS:
                task.setSyncStatus(SyncStatus.TABLE_INIT.getType());
                nodeMilestones(nodeId, KPI_TABLE_INIT, (milestone) -> {
                    milestone.setProgress(aspect.getCompletedCounts());
                    milestone.setTotals(aspect.getTotals());
                });
                break;
            case TableInitFuncAspect.STATE_END:
                Throwable error = aspect.getThrowable();
                if (null == error) {
                    task.setSyncStatus(SyncStatus.TABLE_INIT_FAILED.getType());
                    nodeMilestones(nodeId, KPI_TABLE_INIT, (milestone) -> {
                        milestone.setProgress(aspect.getTotals());
                        milestone.setTotals(aspect.getTotals());
                        setFinish(milestone);
                    });
                } else {
                    task.setSyncStatus(SyncStatus.TABLE_INIT.getType());
                    nodeMilestones(nodeId, KPI_TABLE_INIT, getErrorConsumer(error.getMessage()));
                }
                break;
            default:
                break;
        }
        return null;
    }

    protected void storeMilestone() {
        try {
            // set task table init
            if (!TaskDto.SYNC_TYPE_LOG_COLLECTOR.equals(task.getSyncType())) {
                taskMilestone(KPI_TABLE_INIT, (milestone) -> {
                    milestone.setStatus(MilestoneStatus.WAITING);
                    if (targetNodes.isEmpty()) return;

                    milestone.setBegin(System.currentTimeMillis());
                    milestone.setEnd(0L);
                    milestone.setStatus(MilestoneStatus.RUNNING);
                    AtomicLong totals = new AtomicLong(0), completed = new AtomicLong(0);
                    for (String nid : targetNodes) {
                        nodeMilestones(nid, KPI_TABLE_INIT, (m) -> {
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

                    if (MilestoneStatus.ERROR != milestone.getStatus() && totals.get() == completed.get()) {
                        milestone.setStatus(MilestoneStatus.FINISH);
                    }
                });
            }

            taskMilestone(KPI_DATA_NODE_INIT, m -> {
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
                    if (null == m.getBegin()) { // update first time
                        m.setBegin(System.currentTimeMillis());
                    }
                    m.setStatus(0 == progress ? MilestoneStatus.WAITING : MilestoneStatus.RUNNING);
                }
                m.setTotals(totals);
                m.setProgress(progress);
            });

            clientMongoOperator.update(
                    Query.query(Criteria.where("_id").is(task.getId()))
                    , Update.update("attrs.milestone", milestones).set("attrs.nodeMilestones", nodeMilestones)
                            .set("syncStatus", task.getSyncStatus())
                    , ConnectorConstant.TASK_COLLECTION);
        } catch (Exception e) {
					if (TmUnavailableException.notInstance(e)) {
						log.warn("Save milestone failed: {}", e.getMessage(), e);
					}
        }
    }

    protected void taskMilestone(String code, Consumer<MilestoneEntity> consumer) {
        MilestoneEntity entity = milestones.get(code);
        if (null == entity) {
            synchronized (milestones) {
                entity = milestones.computeIfAbsent(code, s -> new MilestoneEntity(code, MilestoneStatus.WAITING));
            }
        }
        if (null != consumer) consumer.accept(entity);
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
            nodeKpi(nodeId, code, m -> {
                consumer.accept(aspect, m);
            });
            return null;
        });
    }

    protected void nodeKpi(String nodeId, String code, Consumer<MilestoneEntity> consumer) {
        Map<String, MilestoneEntity> nodeMap = nodeMilestones.get(nodeId);
        if (null == nodeMap) {
            synchronized (nodeMilestones) {
                nodeMap = nodeMilestones.computeIfAbsent(nodeId, s -> new HashMap<>());
            }
        }

        MilestoneEntity entity = nodeMap.computeIfAbsent(code, s -> new MilestoneEntity(code, MilestoneStatus.WAITING));
        consumer.accept(entity);
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
        m.setEnd(System.currentTimeMillis());
        m.setStatus(MilestoneStatus.ERROR);
        m.setErrorMessage(Optional.ofNullable(aspect.getError()).map(Throwable::getMessage).orElse(null));
    }

    protected Consumer<MilestoneEntity> getErrorConsumer(String errorMessage) {
        return (m) -> {
            m.setEnd(System.currentTimeMillis());
            m.setStatus(MilestoneStatus.ERROR);
            m.setErrorMessage(errorMessage);
        };
    }

}
