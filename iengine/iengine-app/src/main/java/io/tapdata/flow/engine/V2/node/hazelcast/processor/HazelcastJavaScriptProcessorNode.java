package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.oracle.truffle.js.scriptengine.GraalJSScriptEngine;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.JavaScriptFunctions;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.processor.constant.JSEngineEnum;
import com.tapdata.processor.context.ProcessContext;
import com.tapdata.processor.context.ProcessContextEvent;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.CacheLookupProcessorNode;
import com.tapdata.tm.commons.dag.process.JsProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateJsProcessorNode;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.flow.engine.V2.script.ObsScriptLogger;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.SneakyThrows;
import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Query;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.springframework.data.mongodb.core.query.Criteria.where;

public class HazelcastJavaScriptProcessorNode extends HazelcastProcessorBaseNode{

  private static final Logger logger = LogManager.getLogger(HazelcastJavaScriptProcessorNode.class);
  public static final String TAG = HazelcastJavaScriptProcessorNode.class.getSimpleName();

  private final Invocable engine;

  private final ThreadLocal<Map<String, Object>> processContextThreadLocal;

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

    List<JavaScriptFunctions> javaScriptFunctions = clientMongoOperator.find(
            new Query(where("type").ne("system")).with(Sort.by(Sort.Order.asc("last_update"))),
            ConnectorConstant.JAVASCRIPT_FUNCTION_COLLECTION, JavaScriptFunctions.class);

      this.engine = ScriptUtil.getScriptEngine(
              JSEngineEnum.GRAALVM_JS.getEngineName(),
              script, javaScriptFunctions,
              clientMongoOperator,
              null,
              null,
              ((DataProcessorContext) processorBaseContext).getCacheService(),
              new ObsScriptLogger(obsLogger)
      );

    this.processContextThreadLocal = ThreadLocal.withInitial(HashMap::new);
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
    Map<String, Object> context = this.processContextThreadLocal.get();
    context.putAll(contextMap);
    ((ScriptEngine) this.engine).put("context", context);
    Object obj = engine.invokeFunction(ScriptUtil.FUNCTION_NAME, record);
    context.clear();

    if (obj == null) {
      if (logger.isDebugEnabled()) {
        logger.debug("The event does not need to continue to be processed {}", tapdataEvent);
      }
    } else if (obj instanceof List) {
      for (Object o : (List) obj) {
        Map<String, Object> recordMap = new HashMap<>();
        MapUtil.copyToNewMap((Map<String, Object>) o, recordMap);
        TapdataEvent cloneTapdataEvent = (TapdataEvent) tapdataEvent.clone();
        setRecordMap(cloneTapdataEvent.getTapEvent(), op, recordMap);
        consumer.accept(cloneTapdataEvent, processResult);
      }
    } else {
      Map<String, Object> recordMap = new HashMap<>();
      MapUtil.copyToNewMap((Map<String, Object>) obj, recordMap);
      setRecordMap(tapEvent, op, recordMap);
      consumer.accept(tapdataEvent, processResult);
    }
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
