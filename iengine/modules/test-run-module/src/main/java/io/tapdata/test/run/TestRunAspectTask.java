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
import io.tapdata.aspect.task.AspectTask;
import io.tapdata.aspect.task.AspectTaskSession;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.simplify.pretty.ClassHandlers;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@AspectTaskSession(includeTypes = TaskDto.SYNC_TYPE_TEST_RUN)
public class TestRunAspectTask extends AspectTask {

  private final ClassHandlers observerClassHandlers = new ClassHandlers();

  private Set<String> nodeIds;

  private final Map<String, List<Map<String, Object>>> resultMap = new ConcurrentHashMap<>();

  public TestRunAspectTask() {
    observerClassHandlers.register(ProcessorNodeProcessAspect.class, this::processorNodeProcessAspect);
  }

  @Override
  public void onStart() {
    Optional<Node> optional = task.getDag().getNodes().stream().filter(n -> n.getType().equals("virtualTarget")).findFirst();
    optional.ifPresent(node -> this.nodeIds = task.getDag().getPreNodes(node.getId()).stream()
            .map(Element::getId).collect(Collectors.toSet()));
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
          resultMap.computeIfAbsent("before", key -> new ArrayList<>()).add(getMap(inputEvent));
          processAspect.setConsumer(outputEvent ->
                  resultMap.computeIfAbsent("after", key -> new ArrayList<>()).add(getMap(outputEvent)));
        }
        break;
      case ProcessorFunctionAspect.STATE_END:
          break;
    }


    return null;
  }

  private Map<String, Object> getMap(TapdataEvent tapdataEvent) {
    TapEvent tapEvent = (TapEvent) tapdataEvent.getTapEvent().clone();
    if (tapEvent instanceof TapInsertRecordEvent) {
      return ((TapInsertRecordEvent) tapEvent).getAfter();
    }
    return new HashMap<>();
  }

  @Override
  public void onStop() {
    ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
    Map<String, Object> paramMap = new HashMap<>();
    paramMap.put("taskId", task.getParentTask().getId().toHexString());
    paramMap.put("code", "ok");
    paramMap.put("ts", new Date().getTime());
    paramMap.put("before", resultMap.get("before"));
    paramMap.put("after", resultMap.get("after"));

    clientMongoOperator.insertOne(paramMap, "/task/migrate-js/save-result");

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
}
