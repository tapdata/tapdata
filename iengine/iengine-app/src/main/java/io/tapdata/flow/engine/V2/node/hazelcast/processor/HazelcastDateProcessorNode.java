package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.*;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.DateProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateDateProcessorNode;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.schema.TapTableMap;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;


public class HazelcastDateProcessorNode extends HazelcastProcessorBaseNode {

  private static final Logger logger = LogManager.getLogger(HazelcastDateProcessorNode.class);
  public static final String TAG = HazelcastDateProcessorNode.class.getSimpleName();


  /** 需要修改时间的类型 */
  private List<String> dataTypes;
  /** 增加或者减少 */
  private boolean add;
  /** 增加或者减少的小时数 */
  private int hours;

  @SneakyThrows
  public HazelcastDateProcessorNode(ProcessorBaseContext processorBaseContext) {
    super(processorBaseContext);
    initConfig();
  }


  private void initConfig() {
    Node node = getNode();
    if (node instanceof DateProcessorNode) {
      this.dataTypes = ((DateProcessorNode) node).getDataTypes();
      this.add = ((DateProcessorNode) node).isAdd();
      this.hours = ((DateProcessorNode) node).getHours();
    } else if (node instanceof MigrateDateProcessorNode) {
      this.dataTypes = ((MigrateDateProcessorNode) node).getDataTypes();
      this.add = ((MigrateDateProcessorNode) node).isAdd();
      this.hours = ((MigrateDateProcessorNode) node).getHours();
    }
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

    TapTable tapTable = processorBaseContext.getTapTableMap().get(tableName);


    TapRecordEvent recordEvent = (TapRecordEvent) tapEvent;

    Map<String, Object> info = recordEvent.getInfo();


  }
}