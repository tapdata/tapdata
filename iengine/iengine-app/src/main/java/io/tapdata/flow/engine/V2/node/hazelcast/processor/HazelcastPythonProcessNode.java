package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.cache.scripts.ScriptCacheService;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.OperationType;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.processor.context.ProcessContext;
import com.tapdata.processor.context.ProcessContextEvent;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.process.script.py.PyProcessNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.Application;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.script.ObsScriptLogger;
import io.tapdata.flow.engine.V2.script.ScriptExecutorsManager;
import io.tapdata.flow.engine.V2.util.GraphUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.SneakyThrows;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.mongodb.core.query.Query;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import java.io.Closeable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author GavinXiao
 * @description HazelcastPythonProcessNode create by Gavin
 * @create 2023/6/13 19:24
 * @doc https://iowiki.com/jython/jython_importing_java_libraries.html
 **/
public class HazelcastPythonProcessNode extends HazelcastProcessorBaseNode {
    private static final Logger logger = LogManager.getLogger(HazelcastPythonProcessNode.class);
    public static final String TAG = HazelcastPythonProcessNode.class.getSimpleName();
    private ScriptExecutorsManager scriptExecutorsManager;
    private ScriptExecutorsManager.ScriptExecutor source;
    private ScriptExecutorsManager.ScriptExecutor target;
    private ThreadLocal<Map<String, Object>> processContextThreadLocal;
    private Map<String, Object> globalMap;
    private Invocable engine;

    @SneakyThrows
    public HazelcastPythonProcessNode(ProcessorBaseContext processorBaseContext) {
        super(processorBaseContext);
        Node<?> node = getNode();
        if (!node.disabledNode()) {
            String script;
            if (node instanceof PyProcessNode) {
                script = ((PyProcessNode) node).getScript();
            } else {
                throw new CoreException("unsupported node " + node.getClass().getName());
            }

            //@todo initPythonBuildInMethod and add python function from mongo db
            ScriptCacheService scriptCacheService = new ScriptCacheService(clientMongoOperator, (DataProcessorContext) processorBaseContext);
            this.engine = ScriptUtil.getPyEngine(
                    ScriptFactory.TYPE_PYTHON,
                    script,
                    null, //javaScriptFunctions,
                    clientMongoOperator,
                    null,
                    null,
                    scriptCacheService,
                    new ObsScriptLogger(obsLogger, logger),
                    Application.class.getClassLoader());
            this.processContextThreadLocal = ThreadLocal.withInitial(HashMap::new);
            this.globalMap = new HashMap<>();
        }
    }

    @Override
    protected void doInit(@NotNull Context context) throws Exception {
        super.doInit(context);
        Node<?> node = getNode();
        if (!node.disabledNode()) {
            this.scriptExecutorsManager = new ScriptExecutorsManager(
                    new ObsScriptLogger(obsLogger),
                    clientMongoOperator,
                    jetContext.hazelcastInstance(),
                    node.getTaskId(),
                    node.getId(),
                    StringUtils.equalsAnyIgnoreCase(
                            processorBaseContext.getTaskDto().getSyncType(),
                            TaskDto.SYNC_TYPE_TEST_RUN,
                            TaskDto.SYNC_TYPE_DEDUCE_SCHEMA
                    )
            );
            ((ScriptEngine) this.engine).put("ScriptExecutorsManager", scriptExecutorsManager);
            this.source = getDefaultScriptExecutor(GraphUtil.predecessors(node, Node::isDataNode), "source");
            this.target = getDefaultScriptExecutor(GraphUtil.successors(node, Node::isDataNode), "target");
            ((ScriptEngine) this.engine).put("source", source);
            ((ScriptEngine) this.engine).put("target", target);
        }
    }

    @SneakyThrows
    @Override
    protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
        TapEvent tapEvent = tapdataEvent.getTapEvent();
        String tableName = TapEventUtil.getTableId(tapEvent);
        ProcessResult processResult = getProcessResult(tableName);
        if (disabledNode() || !(tapEvent instanceof TapRecordEvent)) {
            consumer.accept(tapdataEvent, processResult);
            return;
        }

        Map<String, Object> afterMapInRecord = TapEventUtil.getAfter(tapEvent);
        if (MapUtils.isEmpty(afterMapInRecord) && MapUtils.isNotEmpty(TapEventUtil.getBefore(tapEvent))) {
            afterMapInRecord = TapEventUtil.getBefore(tapEvent);
        }

        String op = TapEventUtil.getOp(tapEvent);
        ProcessContext processContext = new ProcessContext(op, tableName, null, null, null, tapdataEvent.getOffset());

        Long referenceTime = ((TapRecordEvent) tapEvent).getReferenceTime();
        long eventTime = referenceTime == null ? 0 : referenceTime;
        processContext.setEventTime(eventTime);
        processContext.setTs(eventTime);
        SyncStage syncStage = tapdataEvent.getSyncStage();
        processContext.setType(syncStage == null ? SyncStage.INITIAL_SYNC.name() : syncStage.name());
        processContext.setSyncType(getProcessorBaseContext().getTaskDto().getSyncType());

        ProcessContextEvent processContextEvent = processContext.getEvent();
        if (processContextEvent == null) {
            processContextEvent = new ProcessContextEvent(op, tableName, processContext.getSyncType(), eventTime);
        }
        Map<String, Object> before = TapEventUtil.getBefore(tapEvent);
        if (null != before) {
            processContextEvent.setBefore(before);
        }
        processContextEvent.setType(processContext.getType());
        Map<String, Object> eventMap = MapUtil.obj2Map(processContextEvent);
        Map<String, Object> contextMap = MapUtil.obj2Map(processContext);
        contextMap.put("event", eventMap);
        contextMap.put("before", before);
        contextMap.put("info", tapEvent.getInfo());
        contextMap.put("global", this.globalMap);
        Map<String, Object> context = this.processContextThreadLocal.get();
        context.putAll(contextMap);
        AtomicReference<Object> scriptInvokeResult = new AtomicReference<>();
        if (StringUtils.equalsAnyIgnoreCase(processorBaseContext.getTaskDto().getSyncType(),
                TaskDto.SYNC_TYPE_TEST_RUN,
                TaskDto.SYNC_TYPE_DEDUCE_SCHEMA)) {
            Map<String, Object> finalRecord = afterMapInRecord;
            CountDownLatch countDownLatch = new CountDownLatch(1);
            AtomicReference<Throwable> errorAtomicRef = new AtomicReference<>();
            Thread thread = new Thread(() -> {
                Thread.currentThread().setName("Python-Test-Runner");
                try {
                    scriptInvokeResult.set(engine.invokeFunction(ScriptUtil.FUNCTION_NAME, finalRecord, context));
                } catch (Exception throwable) {
                    errorAtomicRef.set(throwable);
                } finally {
                    countDownLatch.countDown();
                }
            });
            thread.start();
            boolean threadFinished = countDownLatch.await(10L, TimeUnit.SECONDS);
            if (!threadFinished) {
                thread.interrupt();
            }
            if (errorAtomicRef.get() != null) {
                throw new TapCodeException(TaskProcessorExCode_11.PYTHON_PROCESS_FAILED, errorAtomicRef.get());
            }

        } else {
            scriptInvokeResult.set(engine.invokeFunction(ScriptUtil.FUNCTION_NAME, afterMapInRecord, context));
        }

        if (StringUtils.isNotEmpty((CharSequence) context.get("op"))) {
            op = (String) context.get("op");
        }

        context.clear();
        final String finalOp = op;
        Object result = scriptInvokeResult.get();
        if (null == result) {
            if (logger.isDebugEnabled()) {
                logger.debug("The event does not need to continue to be processed {}", tapdataEvent);
            }
        } else if (result instanceof Collection) {
            Collection<?> collection = (Collection<?>) result;
            for (Object o : collection) {
                Map<String, Object> recordMap = new HashMap<>();
                MapUtil.copyToNewMap((Map<String, Object>) o, recordMap);
                TapdataEvent cloneTapdataEvent = (TapdataEvent) tapdataEvent.clone();
                TapEvent returnTapEvent = getTapEvent(cloneTapdataEvent.getTapEvent(), finalOp);
                setRecordMap(returnTapEvent, finalOp, recordMap);
                cloneTapdataEvent.setTapEvent(returnTapEvent);
                consumer.accept(cloneTapdataEvent, processResult);
            }
        } else if (result instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) result;
            Map<String, Object> recordMap = new HashMap<>();
            MapUtil.copyToNewMap(map, recordMap);
            TapEvent returnTapEvent = getTapEvent(tapEvent, finalOp);
            setRecordMap(returnTapEvent, finalOp, recordMap);
            tapdataEvent.setTapEvent(returnTapEvent);
            consumer.accept(tapdataEvent, processResult);
        }
    }

    @Override
    protected void doClose() throws Exception {
        try {
            CommonUtils.ignoreAnyError(() -> Optional.ofNullable(this.source).ifPresent(ScriptExecutorsManager.ScriptExecutor::close), TAG);
            CommonUtils.ignoreAnyError(() -> Optional.ofNullable(this.target).ifPresent(ScriptExecutorsManager.ScriptExecutor::close), TAG);
            CommonUtils.ignoreAnyError(() -> Optional.ofNullable(this.scriptExecutorsManager).ifPresent(ScriptExecutorsManager::close), TAG);
            CommonUtils.ignoreAnyError(() -> {
                if (this.engine instanceof Closeable) {
                    ((Closeable)this.engine).close();
                }
            }, TAG);
            CommonUtils.ignoreAnyError(() -> Optional.ofNullable(processContextThreadLocal).ifPresent(ThreadLocal::remove), TAG);
        } finally {
            super.doClose();
        }
    }

    private ScriptExecutorsManager.ScriptExecutor getDefaultScriptExecutor(List<Node<?>> nodes, String flag) {
        if (nodes != null && !nodes.isEmpty()) {
            Node<?> node = nodes.get(0);
            if (node instanceof DataParentNode) {
                String connectionId = ((DataParentNode<?>) node).getConnectionId();
                Connections connections = clientMongoOperator.findOne(new Query(where("_id").is(connectionId)),
                        ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
                if (connections != null) {
                    if (nodes.size() > 1) {
                        obsLogger.warn("Use the first node as the default python executor, please use it with caution.");
                    }
                    return this.scriptExecutorsManager.create(connections, clientMongoOperator, jetContext.hazelcastInstance(), new ObsScriptLogger(obsLogger));
                }
            }
        }
        obsLogger.warn("The " + flag + " could not build the executor, please check");
        return null;
    }

    private TapEvent getTapEvent(TapEvent tapEvent, String op) {
        if (StringUtils.equals(TapEventUtil.getOp(tapEvent), op)) {
            return tapEvent;
        }
        OperationType operationType = OperationType.fromOp(op);
        TapEvent result;

        switch (operationType) {
            case INSERT:
                result = TapInsertRecordEvent.create();
                break;
            case UPDATE:
                result = TapUpdateRecordEvent.create();
                break;
            case DELETE:
                result = TapDeleteRecordEvent.create();
                break;
            default:
                throw new IllegalArgumentException("Unsupported operation type: " + op);
        }
        tapEvent.clone(result);
        return result;
    }

    private static void setRecordMap(TapEvent tapEvent, String op, Map<String, Object> recordMap) {
        if (ConnectorConstant.MESSAGE_OPERATION_DELETE.equals(op)) {
            TapEventUtil.setBefore(tapEvent, recordMap);
        } else {
            TapEventUtil.setAfter(tapEvent, recordMap);
        }
    }
}
