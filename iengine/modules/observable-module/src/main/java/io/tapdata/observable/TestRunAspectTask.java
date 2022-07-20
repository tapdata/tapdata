package io.tapdata.observable;

import com.tapdata.constant.BeanUtil;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Element;
import com.tapdata.tm.commons.dag.Node;
import io.tapdata.aspect.ProcessorNodeProcessAspect;
import io.tapdata.aspect.task.AspectTask;
import io.tapdata.aspect.task.AspectTaskSession;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.simplify.pretty.ClassHandlers;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@AspectTaskSession
public class TestRunAspectTask extends AspectTask {

  private final ClassHandlers observerClassHandlers = new ClassHandlers();

  private Set<String> nodeIds;

  private final Map<String, List<Map<String, Object>>> resultMap = new ConcurrentHashMap<>();

  @Override
  public void onStart() {
    observerClassHandlers.register(ProcessorNodeProcessAspect.class, this::processorNodeProcessAspect);
    Optional<Node> optional = task.getDag().getNodes().stream().filter(n -> n.getType().equals("virtualTarget")).findFirst();
    optional.ifPresent(node -> this.nodeIds = task.getDag().getPreNodes(node.getId()).stream()
            .map(Element::getId).collect(Collectors.toSet()));
  }

  private Void processorNodeProcessAspect(ProcessorNodeProcessAspect t) {
    ProcessorBaseContext processorBaseContext = t.getProcessorBaseContext();

    String nodeId = processorBaseContext.getNode().getId();
    if (nodeIds.contains(nodeId)) {
      TapdataEvent inputEvent = t.getInputEvent();
      TapdataEvent outputEvent = t.getOutputEvent();
      /**
       * {"before":[{}], "after":[{}]}
       */
      resultMap.computeIfAbsent("before", key -> new ArrayList<>()).add(getMap(inputEvent));
      resultMap.computeIfAbsent("after", key -> new ArrayList<>()).add(getMap(outputEvent));
    }
    return null;
  }

  private Map<String, Object> getMap(TapdataEvent tapdataEvent) {
    if (tapdataEvent.getTapEvent() instanceof TapInsertRecordEvent) {
      return ((TapInsertRecordEvent) tapdataEvent.getTapEvent()).getAfter();
    }
    return new HashMap<>();
  }

  @Override
  public void onStop() {
    //todo
    ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);

  }

  @Override
  public List<Class<? extends Aspect>> observeAspects() {
    return null;
  }

  @Override
  public List<Class<? extends Aspect>> interceptAspects() {
    return null;
  }

  @Override
  public void onObserveAspect(Aspect aspect) {

  }

  @Override
  public AspectInterceptResult onInterceptAspect(Aspect aspect) {
    return null;
  }
}
