package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.oracle.truffle.js.scriptengine.GraalJSScriptEngine;
import com.tapdata.cache.scripts.ScriptCacheService;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.*;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.processor.constant.JSEngineEnum;
import com.tapdata.processor.context.ProcessContext;
import com.tapdata.processor.context.ProcessContextEvent;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.process.*;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
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
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Query;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.springframework.data.mongodb.core.query.Criteria.where;

public class HazelcastJavaScriptProcessorNode extends HazelcastProcessorBaseNode {

  private static final Logger logger = LogManager.getLogger(HazelcastJavaScriptProcessorNode.class);
  public static final String TAG = HazelcastJavaScriptProcessorNode.class.getSimpleName();

  private final Invocable engine;

  private ScriptExecutorsManager scriptExecutorsManager;

  private final ThreadLocal<Map<String, Object>> processContextThreadLocal;
  private ScriptExecutorsManager.ScriptExecutor source;
  private ScriptExecutorsManager.ScriptExecutor target;

  /**
   * standard js
   */
  private boolean standard;

  @SneakyThrows
  public HazelcastJavaScriptProcessorNode(ProcessorBaseContext processorBaseContext) {
    super(processorBaseContext);
    Node node = getNode();
    String script;
    if (node instanceof JsProcessorNode) {
      script = ((JsProcessorNode) node).getScript();
    } else if (node instanceof MigrateJsProcessorNode) {
      script = ((MigrateJsProcessorNode) node).getScript();
    } else if (node instanceof CacheLookupProcessorNode) {
      script = ((CacheLookupProcessorNode) node).getScript();
    } else {
      throw new RuntimeException("unsupported node " + node.getClass().getName());
    }

    if (node instanceof StandardJsProcessorNode || node instanceof StandardMigrateJsProcessorNode) {
      this.standard = true;
    }

    List<JavaScriptFunctions> javaScriptFunctions;
    if (standard) {
      javaScriptFunctions = null;
    } else {
      javaScriptFunctions = clientMongoOperator.find(new Query(where("type").ne("system")).with(Sort.by(Sort.Order.asc("last_update"))),
              ConnectorConstant.JAVASCRIPT_FUNCTION_COLLECTION, JavaScriptFunctions.class);
    }

    ScriptCacheService scriptCacheService = new ScriptCacheService(clientMongoOperator, (DataProcessorContext) processorBaseContext);

    this.engine = ScriptUtil.getScriptEngine(
            JSEngineEnum.GRAALVM_JS.getEngineName(),
            script, javaScriptFunctions,
            clientMongoOperator,
            null,
            null,
            scriptCacheService,
            new ObsScriptLogger(obsLogger, logger),
            this.standard
    );

    this.processContextThreadLocal = ThreadLocal.withInitial(HashMap::new);
  }

  @Override
  protected void doInit(@NotNull Context context) throws Exception {
    super.doInit(context);
    Node node = getNode();
    if (!this.standard) {
      this.scriptExecutorsManager = new ScriptExecutorsManager(new ObsScriptLogger(obsLogger), clientMongoOperator, jetContext.hazelcastInstance(),
              node.getTaskId(), node.getId());
      ((ScriptEngine) this.engine).put("ScriptExecutorsManager", scriptExecutorsManager);

      List<Node<?>> predecessors = GraphUtil.predecessors(node, Node::isDataNode);
      List<Node<?>> successors = GraphUtil.successors(node, Node::isDataNode);

      this.source = getDefaultScriptExecutor(predecessors, "source");
      this.target = getDefaultScriptExecutor(successors, "target");
      ((ScriptEngine) this.engine).put("source", source);
      ((ScriptEngine) this.engine).put("target", target);
    }

  }

  private ScriptExecutorsManager.ScriptExecutor getDefaultScriptExecutor(List<Node<?>> nodes, String flag) {
    if (nodes != null && nodes.size() > 0) {
      Node<?> node = nodes.get(0);
      if (node instanceof DataParentNode) {
        String connectionId = ((DataParentNode) node).getConnectionId();
        Connections connections = clientMongoOperator.findOne(new Query(where("_id").is(connectionId)),
                ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
        if (connections != null) {
          if (nodes.size() > 1) {
            obsLogger.warn("Use the first node as the default script executor, please use it with caution.");
          }
          return new ScriptExecutorsManager.ScriptExecutor(connections, clientMongoOperator, jetContext.hazelcastInstance(), new ObsScriptLogger(obsLogger), TAG + "_" + node.getId());
        }
      }
    }
    obsLogger.warn("The " + flag + " could not build the executor, please check");
    return null;
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

    Map<String, Object> record = TapEventUtil.getAfter(tapEvent);
    if (MapUtils.isEmpty(record) && MapUtils.isNotEmpty(TapEventUtil.getBefore(tapEvent))) {
      record = TapEventUtil.getBefore(tapEvent);
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
    Map<String, Object> context = this.processContextThreadLocal.get();
    context.putAll(contextMap);
    ((ScriptEngine) this.engine).put("context", context);
    AtomicReference<Object> scriptInvokeResult = new AtomicReference<>();
    if (StringUtils.equalsAnyIgnoreCase(processorBaseContext.getTaskDto().getSyncType(),
            TaskDto.SYNC_TYPE_TEST_RUN,
            TaskDto.SYNC_TYPE_DEDUCE_SCHEMA)) {
      Map<String, Object> finalRecord = record;
      CountDownLatch countDownLatch = new CountDownLatch(1);
			AtomicReference<Throwable> errorAtomicRef = new AtomicReference<>();
      Thread thread = new Thread(() -> {
        Thread.currentThread().setName("Javascript-Test-Runner");
        try {
          scriptInvokeResult.set(engine.invokeFunction(ScriptUtil.FUNCTION_NAME, finalRecord));
        } catch (Throwable throwable) {
					errorAtomicRef.set(throwable);
        } finally {
          countDownLatch.countDown();
        }
      });
      thread.start();
      boolean threadFinished = countDownLatch.await(10L, TimeUnit.SECONDS);
      if (!threadFinished) {
        thread.stop();
      }
			if (errorAtomicRef.get() != null) {
				throw new TapCodeException(TaskProcessorExCode_11.JAVA_SCRIPT_PROCESS_FAILED, errorAtomicRef.get());
			}

    } else {
      scriptInvokeResult.set(engine.invokeFunction(ScriptUtil.FUNCTION_NAME, record));
    }

    if (StringUtils.isNotEmpty((CharSequence) context.get("op"))) {
      op = (String) context.get("op");
    }

    context.clear();

    if (null == scriptInvokeResult.get()) {
      if (logger.isDebugEnabled()) {
        logger.debug("The event does not need to continue to be processed {}", tapdataEvent);
      }
    } else if (scriptInvokeResult.get() instanceof List) {
      for (Object o : (List) scriptInvokeResult.get()) {
        Map<String, Object> recordMap = new HashMap<>();
        MapUtil.copyToNewMap((Map<String, Object>) o, recordMap);
        TapdataEvent cloneTapdataEvent = (TapdataEvent) tapdataEvent.clone();
        TapEvent returnTapEvent = getTapEvent(cloneTapdataEvent.getTapEvent(), op);
        setRecordMap(returnTapEvent, op, recordMap);
        cloneTapdataEvent.setTapEvent(returnTapEvent);
        consumer.accept(cloneTapdataEvent, processResult);
      }
    } else {
      Map<String, Object> recordMap = new HashMap<>();
      MapUtil.copyToNewMap((Map<String, Object>) scriptInvokeResult.get(), recordMap);
      TapEvent returnTapEvent = getTapEvent(tapEvent, op);
      setRecordMap(returnTapEvent, op, recordMap);
      tapdataEvent.setTapEvent(returnTapEvent);
      consumer.accept(tapdataEvent, processResult);
    }
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

  @Override
  protected void doClose() throws Exception {
    try {
      CommonUtils.ignoreAnyError(() -> Optional.ofNullable(this.source).ifPresent(ScriptExecutorsManager.ScriptExecutor::close), TAG);
      CommonUtils.ignoreAnyError(() -> Optional.ofNullable(this.target).ifPresent(ScriptExecutorsManager.ScriptExecutor::close), TAG);
      CommonUtils.ignoreAnyError(() -> Optional.ofNullable(this.scriptExecutorsManager).ifPresent(ScriptExecutorsManager::close), TAG);
      CommonUtils.ignoreAnyError(() -> {
        if (this.engine instanceof GraalJSScriptEngine) {
          ((GraalJSScriptEngine) this.engine).close();
        }
      }, TAG);
    } finally {
      super.doClose();
    }
  }
}
