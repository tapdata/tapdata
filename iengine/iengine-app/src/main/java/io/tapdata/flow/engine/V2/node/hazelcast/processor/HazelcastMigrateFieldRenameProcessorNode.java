package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.constant.MapUtil;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.vo.FieldInfo;
import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class HazelcastMigrateFieldRenameProcessorNode  extends HazelcastProcessorBaseNode{

  /**
   * key: table name
   *  --key old field name
   */
  private final Map<String, Map<String, FieldInfo>> tableFieldsMappingMap;

  public HazelcastMigrateFieldRenameProcessorNode(ProcessorBaseContext processorBaseContext) {
    super(processorBaseContext);
    MigrateFieldRenameProcessorNode migrateFieldRenameProcessorNode = (MigrateFieldRenameProcessorNode) getNode();
    this.tableFieldsMappingMap = migrateFieldRenameProcessorNode.getFieldsMapping().stream()
            .collect(Collectors.toMap(TableFieldInfo::getPreviousTableName,
                    t->t.getFields().stream()
                            .collect(Collectors.toMap(FieldInfo::getSourceFieldName, Function.identity()))));
  }

  @Override
  protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {

    TapEvent tapEvent = tapdataEvent.getTapEvent();
    String tableId = TapEventUtil.getTableId(tapEvent);

    Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
    Map<String, Object> before = TapEventUtil.getBefore(tapEvent);
    TapEventUtil.setAfter(tapEvent, processFields(after, tableId));
    TapEventUtil.setBefore(tapEvent, processFields(before, tableId));

    //ddl event todo

    String tableName = tableId;
    if (!multipleTables && StringUtils.equalsAnyIgnoreCase(processorBaseContext.getTaskDto().getSyncType(),
            TaskDto.SYNC_TYPE_DEDUCE_SCHEMA)) {
      tableName = processorBaseContext.getNode().getId();
    }
    consumer.accept(tapdataEvent, ProcessResult.create().tableId(tableName));


  }

  private Map<String, Object> processFields(Map<String, Object> map, String tableName) {
    if (MapUtils.isEmpty(map)) {
      return map;
    }
    if (StringUtils.isEmpty(tableName)) {
      return map;
    }
    Map<String, FieldInfo> fieldsMappingMap = this.tableFieldsMappingMap.get(tableName);
    if (MapUtils.isEmpty(fieldsMappingMap)) {
      return map;
    }

    for (Map.Entry<String, FieldInfo> entry : fieldsMappingMap.entrySet()) {
      if (MapUtil.containsKey(map, entry.getKey())) {
        FieldInfo fieldInfo = entry.getValue();
        if (fieldInfo.getIsShow() != null && !fieldInfo.getIsShow()) {
          MapUtil.removeValueByKey(map, entry.getKey());
          continue;
        }
        Object value = MapUtil.getValueByKey(map, entry.getKey());
        try {
          MapUtil.replaceKey(fieldInfo.getSourceFieldName(), map, fieldInfo.getTargetFieldName());
        } catch (Exception e) {
          throw new RuntimeException("Error when modifying field name: " + fieldInfo + "--" + value, e);
        }
      }
    }
    return map;
  }
}
