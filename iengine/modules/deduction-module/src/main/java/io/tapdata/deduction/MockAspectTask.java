package io.tapdata.deduction;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Element;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.ProcessorFunctionAspect;
import io.tapdata.aspect.ProcessorNodeProcessAspect;
import io.tapdata.aspect.TaskStartAspect;
import io.tapdata.aspect.task.AbstractAspectTask;
import io.tapdata.aspect.task.AspectTaskSession;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.schema.SampleMockUtil;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@AspectTaskSession(
        includeTypes = {TaskDto.SYNC_TYPE_DEDUCE_SCHEMA, TaskDto.SYNC_TYPE_TEST_RUN},
        order = Integer.MIN_VALUE)
public class MockAspectTask extends AbstractAspectTask {

  private Set<String> nodeIds;

  private boolean multipleTables;

  public MockAspectTask() {
    observerHandlers.register(ProcessorNodeProcessAspect.class, this::processorNodeProcessAspect);
  }

  private Void processorNodeProcessAspect(ProcessorNodeProcessAspect processAspect) {
    ProcessorBaseContext processorBaseContext = processAspect.getProcessorBaseContext();
    String nodeId = processorBaseContext.getNode().getId();
    if (nodeIds.contains(nodeId)) {
      switch (processAspect.getState()) {
        case ProcessorNodeProcessAspect.STATE_START:
          break;
        case ProcessorFunctionAspect.STATE_END:
          TapdataEvent inputEvent = processAspect.getInputEvent();
          TapEvent tapEvent = inputEvent.getTapEvent();
          if (!(tapEvent instanceof TapRecordEvent)) {
            return null;
          }
          String tableId = TapEventUtil.getTableId(tapEvent);
          if (!multipleTables) {
            tableId = nodeId;
          }
          TapTable tapTable = processorBaseContext.getTapTableMap().get(tableId);
          SampleMockUtil.mock(tapTable, TapEventUtil.getAfter(tapEvent));
          break;
      }
    }
    return null;
  }

  @Override
  public void onStart(TaskStartAspect startAspect) {
    Optional<Node> optional = task.getDag().getNodes().stream().filter(n -> null != n && "virtualTarget".equals(n.getType())).findFirst();
    if (optional.isPresent()) {
      Node virtualTargetNode = optional.get();
      List<Node> targetNodes = task.getDag().predecessors(virtualTargetNode.getId());
      if (targetNodes == null || targetNodes.size() != 1) {
        throw new IllegalStateException("targetNodes is null or size is not 1");
      }
      Node targetNode = targetNodes.get(0);
      this.nodeIds = task.getDag().predecessors(targetNode.getId()).stream().map(Element::getId).collect(Collectors.toSet());
    }
    this.multipleTables = CollectionUtils.isNotEmpty(task.getDag().getSourceNode());
  }


}
