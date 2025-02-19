package io.tapdata.test.run;

import com.tapdata.constant.BeanUtil;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Element;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.ProcessorFunctionAspect;
import io.tapdata.aspect.ProcessorNodeProcessAspect;
import io.tapdata.aspect.TaskStartAspect;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.task.AspectTask;
import io.tapdata.aspect.task.AspectTaskSession;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.codec.filter.impl.AllLayerMapIterator;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.simplify.pretty.ClassHandlers;
import io.tapdata.entity.simplify.pretty.ClassHandlersV2;
import io.tapdata.exception.ExceptionUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@AspectTaskSession(includeTypes = TaskDto.SYNC_TYPE_TEST_RUN, order = Integer.MAX_VALUE)
public class TestRunAspectTask extends AspectTask {

  private final Logger logger = LogManager.getLogger(TestRunAspectTask.class);

  private final ClassHandlers observerClassHandlers = new ClassHandlers();
  private final ClassHandlersV2 valueHandler;

  private Set<String> nodeIds;

  private final Map<String, List<Map<String, Object>>> resultMap = new ConcurrentHashMap<>();

  private final TapCodecsFilterManager codecsFilterManager;

  private boolean multipleTables;

  private AtomicReference<Object> scriptNodeRunResult;

  //标记是否标准化JS或者Python节点，默认不是，当scriptNodeRunResult不为空则是
  private boolean isNewVersionScriptNodeType = false;

  public TestRunAspectTask() {
    observerClassHandlers.register(ProcessorNodeProcessAspect.class, this::processorNodeProcessAspect);
    TapCodecsRegistry tapCodecsRegistry = TapCodecsRegistry.create();
    tapCodecsRegistry.registerFromTapValue(TapDateTimeValue.class, tapValue -> tapValue.getValue().toInstant().toString());
    codecsFilterManager = TapCodecsFilterManager.create(tapCodecsRegistry);
    valueHandler = new ClassHandlersV2();
    valueHandler.register(Date.class, value -> value.toInstant().toString());
    valueHandler.register(LocalDateTime.class, LocalDateTime::toString);
    valueHandler.register(Instant.class, Instant::toString);
  }

  @Override
  public void onStart(TaskStartAspect startAspect) {
    Optional<Node> optional = task.getDag().getNodes().stream().filter(n -> n.getType().equals("virtualTarget")).findFirst();
    optional.ifPresent(node -> this.nodeIds = task.getDag().predecessors(node.getId()).stream()
            .map(Element::getId).collect(Collectors.toSet()));
    scriptNodeRunResult = (AtomicReference<Object>)startAspect.info("KYE_OF_SCRIPT_RUN_RESULT");
    isNewVersionScriptNodeType = null != scriptNodeRunResult;
    this.multipleTables = CollectionUtils.isNotEmpty(task.getDag().getSourceNode());

  }

  private Void processorNodeProcessAspect(ProcessorNodeProcessAspect processAspect) {
    ProcessorBaseContext processorBaseContext = processAspect.getProcessorBaseContext();
    String nodeId = processorBaseContext.getNode().getId();
    switch (processAspect.getState()) {
      case ProcessorNodeProcessAspect.STATE_START:
        if (nodeIds.contains(nodeId)) {
          TapdataEvent inputEvent = processAspect.getInputEvent();
          /**
           * {"before":[{}], "after":[{}]}
           */
          resultMap.computeIfAbsent("before", key -> new ArrayList<>());
          Map<String, Object> before = transformValue(inputEvent);
          if (null != before) {
            resultMap.get("before").add(before);
          }
          processAspect.consumer(outputEvent -> {
            resultMap.computeIfAbsent("after", key -> new ArrayList<>());
            Map<String, Object> after = transformValue(outputEvent);
            if (null != after) {
              resultMap.get("after").add(after);
            }
          });
        }
        break;
      case ProcessorFunctionAspect.STATE_END:
          break;
    }


    return null;
  }

  @Override
  public void onStop(TaskStopAspect stopAspect) {
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put("taskId", task.getId().toHexString());
    paramMap.put("version", task.getVersion());
    paramMap.put("ts", new Date().getTime());

    paramMap.put("before", Optional.ofNullable(resultMap.get("before")).orElse(new ArrayList<>()));
    paramMap.put("after", Optional.ofNullable(resultMap.get("after")).orElse(new ArrayList<>()));
    if (stopAspect.getError() != null) {
      //run task error
      paramMap.put("code", "error");
      paramMap.put("message", ExceptionUtil.getMessage(stopAspect.getError()));
    } else {
      paramMap.put("code", "ok");
    }

    if (isNewVersionScriptNodeType) {
      scriptNodeRunResult.set(paramMap);
    } else {
      ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
      clientMongoOperator.insertOne(paramMap, "/task/migrate-js/save-result");
    }

    logger.info("return to tm {}", paramMap);
  }

  @Override
  public List<Class<? extends Aspect>> observeAspects() {
    List<Class<?>> classes = observerClassHandlers.keyList();
    List<Class<? extends Aspect>> aspectClasses = new ArrayList<>();
    for(Class<?> clazz : classes) {
      aspectClasses.add((Class<? extends Aspect>) clazz);
    }
    return aspectClasses;
  }

  @Override
  public List<Class<? extends Aspect>> interceptAspects() {
    return null;
  }

  @Override
  public void onObserveAspect(Aspect aspect) {
    observerClassHandlers.handle(aspect);
  }

  @Override
  public AspectInterceptResult onInterceptAspect(Aspect aspect) {
    return null;
  }

  private Map<String, Object> transformFromTapValue(TapdataEvent tapdataEvent) {
    if (null == tapdataEvent.getTapEvent()) return new HashMap<>();
    TapEvent tapEvent = (TapEvent) tapdataEvent.getTapEvent().clone();
    Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
    if (MapUtils.isNotEmpty(after)) {
      codecsFilterManager.transformFromTapValueMap(after);
    }
    return after;
  }

  private Map<String, Object> transformValue(TapdataEvent tapdataEvent) {
    if(null == tapdataEvent.getTapEvent()) return null;
    TapEvent tapEvent = (TapEvent) tapdataEvent.getTapEvent().clone();
    Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
    if (MapUtils.isNotEmpty(after)) {
      AllLayerMapIterator allLayerMapIterator = new AllLayerMapIterator();
      allLayerMapIterator.iterate(after, (key, value, recursive) -> valueHandler.handle(value));
    }
    return after;
  }
}
