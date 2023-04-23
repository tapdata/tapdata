package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.*;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.DateProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateDateProcessorNode;
import com.tapdata.tm.commons.util.JsonUtil;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.mapping.TypeExprResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.error.TaskDateProcessorExCode_17;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.util.TapCodecUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
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

  private DefaultExpressionMatchingMap matchingMap;

  @SneakyThrows
  public HazelcastDateProcessorNode(ProcessorBaseContext processorBaseContext) {
    super(processorBaseContext);
    initConfig();
  }

  @Override
  protected TapCodecsFilterManager initFilterCodec() {
    TapCodecsRegistry tapCodecsRegistry = TapCodecsRegistry.create();
    return TapCodecUtil.getCodecsFilterManager(tapCodecsRegistry);
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


    HashMap<String, Map<String, String>> expressions = new HashMap<>();
    for (String dataType : dataTypes) {
      expressions.put(dataType, new HashMap<>());
    }
    matchingMap = DefaultExpressionMatchingMap.map(JsonUtil.toJson(expressions));
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

    Node node = getNode();
    TapTable tapTable;
    boolean syncTask;
    if (node instanceof DateProcessorNode) {
      tapTable = processorBaseContext.getTapTableMap().get(getNode().getId());
      syncTask = true;
    } else {
      tapTable = processorBaseContext.getTapTableMap().get(tableName);
      syncTask = false;
    }

    if (tapTable == null) {
      throw new TapCodeException(TaskDateProcessorExCode_17.INIT_TARGET_TABLE_TAP_TABLE_NULL, "Table name: "+ tableName + "node id: " + getNode().getId());
    }

    LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
    List<String> addTimeFields = new ArrayList<>();

    if (nameFieldMap != null) {
      nameFieldMap.forEach((k, v) -> {

        String dataType = v.getDataType();
        if (!syncTask) {
          TypeExprResult<DataMap> exprResult = matchingMap.get(v.getDataType());
          if (exprResult != null) {
            dataType = exprResult.getExpression();
          }
        }

        if (dataTypes.contains(dataType)) {
          addTimeFields.add(k);
        }
      });
    }

    final Map<String, Object> after = TapEventUtil.getAfter(tapEvent);

    if (after != null) {
      addTime(addTimeFields, after);
    }
    consumer.accept(tapdataEvent, processResult);
  }

  private void addTime(List<String> addTimeFields, final Map<String, Object> after) {
    Set<String> set = new HashSet<>(after.keySet());
    for (String k : set) {
      Object v = after.get(k);
      if (addTimeFields.contains(k)) {
        if (v instanceof DateTime) {
          v = ((DateTime) v).toInstant();
          if (add) {
            v = ((Instant) v).plus(hours, ChronoUnit.HOURS);
          } else {
            v = ((Instant) v).minus(hours, ChronoUnit.HOURS);
          }
          after.replace(k, new DateTime((Instant) v));
        } else if (v != null) {
          throw new TapCodeException(TaskDateProcessorExCode_17.SELECTED_TYPE_IS_NON_TIME + "type :" + v);
        }
      }
    }
  }
}