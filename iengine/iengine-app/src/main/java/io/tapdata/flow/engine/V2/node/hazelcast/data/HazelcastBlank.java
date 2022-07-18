package io.tapdata.flow.engine.V2.node.hazelcast.data;

import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.entity.TapdataEvent;
import io.tapdata.flow.engine.V2.node.hazelcast.HazelcastBaseNode;
import org.jetbrains.annotations.NotNull;

/**
 * @author samuel
 * @Description
 * @create 2022-03-04 23:34
 **/
public class HazelcastBlank extends HazelcastBaseNode {

  public HazelcastBlank(ProcessorBaseContext processorBaseContext) {
    super(processorBaseContext);
  }

  @Override
  protected void init(@NotNull Context context) throws Exception {
    super.init(context);
  }

  @Override
  protected boolean tryProcess(int ordinal, @NotNull Object item) throws Exception {
    return offer((TapdataEvent) item);
  }
}
