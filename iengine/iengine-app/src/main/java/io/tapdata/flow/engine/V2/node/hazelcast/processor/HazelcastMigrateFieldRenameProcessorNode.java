package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.constant.MapUtil;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.vo.FieldInfo;
import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.entity.FieldAttrChange;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldPrimaryKeyEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class HazelcastMigrateFieldRenameProcessorNode  extends HazelcastProcessorBaseNode {

  private static final Logger logger = LogManager.getLogger(HazelcastMigrateFieldRenameProcessorNode.class);


  /**
   * key: table name
   *  --key old field name
   */
  private final Map<String, Map<String, FieldInfo>> tableFieldsMappingMap;

  public HazelcastMigrateFieldRenameProcessorNode(ProcessorBaseContext processorBaseContext) {
    super(processorBaseContext);
    MigrateFieldRenameProcessorNode migrateFieldRenameProcessorNode = (MigrateFieldRenameProcessorNode) getNode();



    if (CollectionUtils.isNotEmpty(migrateFieldRenameProcessorNode.getFieldsMapping())) {
      this.tableFieldsMappingMap = migrateFieldRenameProcessorNode.getFieldsMapping().stream()
              .collect(Collectors.toMap(TableFieldInfo::getPreviousTableName,
                      t -> t.getFields().stream()
                              .collect(Collectors.toMap(FieldInfo::getSourceFieldName, Function.identity()))));
    } else {
      this.tableFieldsMappingMap = new LinkedHashMap<>();
    }
  }

  @Override
  protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {

    TapEvent tapEvent = tapdataEvent.getTapEvent();
    if (!(tapEvent instanceof TapBaseEvent)) {
      // No processing required
      consumer.accept(tapdataEvent, null);
    }
    AtomicReference<TapdataEvent> processedEvent = new AtomicReference<>();
    String tableId = TapEventUtil.getTableId(tapEvent);
    if (tapEvent instanceof TapRecordEvent) {
      //dml event
      Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
      Map<String, Object> before = TapEventUtil.getBefore(tapEvent);
      TapEventUtil.setAfter(tapEvent, processFields(after, tableId));
      TapEventUtil.setBefore(tapEvent, processFields(before, tableId));

      processedEvent.set(tapdataEvent);

    } else {
      //ddl event
      if (tapEvent instanceof TapAlterFieldAttributesEvent) {
        processField(tapdataEvent, processedEvent, tableId, ((TapAlterFieldAttributesEvent) tapEvent).getFieldName(),
                (newFieldName) -> ((TapAlterFieldAttributesEvent) tapEvent).setFieldName(newFieldName));
      } else if (tapEvent instanceof TapAlterFieldNameEvent) {
        processField(tapdataEvent, processedEvent, tableId, ((TapAlterFieldNameEvent) tapEvent).getNameChange().getBefore(),
                (newFieldName) -> ((TapAlterFieldNameEvent) tapEvent).getNameChange().setBefore(newFieldName));
      } else if (tapEvent instanceof TapDropFieldEvent) {
        processField(tapdataEvent, processedEvent, tableId, ((TapDropFieldEvent) tapEvent).getFieldName(),
                (newFieldName) -> ((TapDropFieldEvent) tapEvent).setFieldName(newFieldName));
      } else if (tapEvent instanceof TapAlterFieldPrimaryKeyEvent) {
        List<FieldAttrChange<List<String>>> primaryKeyChanges = ((TapAlterFieldPrimaryKeyEvent) tapEvent).getPrimaryKeyChanges();
        ListIterator<FieldAttrChange<List<String>>> primaryKeyChangesIter = primaryKeyChanges.listIterator();
        while (primaryKeyChangesIter.hasNext()) {
          FieldAttrChange<List<String>> fieldAttrChange = primaryKeyChangesIter.next();
          List<String> before = fieldAttrChange.getBefore();
          ListIterator<String> fieldsIter = before.listIterator();
          while (fieldsIter.hasNext()) {
            String fieldName = fieldsIter.next();
            processField(tableId, fieldName, (newFieldName) -> {
              if (newFieldName == null) {
                fieldsIter.remove();
              }
              if (!StringUtils.equals(fieldName, newFieldName)) {
                fieldsIter.set(newFieldName);
              }
            });
          }
          if (CollectionUtils.isEmpty(before)) {
            primaryKeyChangesIter.remove();
          }
        }
        if (CollectionUtils.isNotEmpty(primaryKeyChanges)) {
          processedEvent.set(tapdataEvent);
        }
      } else if (tapEvent instanceof TapCreateIndexEvent) {
        List<TapIndex> indexList = ((TapCreateIndexEvent) tapEvent).getIndexList();
        Iterator<TapIndex> indexIterator = indexList.iterator();
        while (indexIterator.hasNext()) {
          TapIndex tapIndex = indexIterator.next();
          List<TapIndexField> indexFields = tapIndex.getIndexFields();
          ListIterator<TapIndexField> tapIndexFieldListIterator = indexFields.listIterator();
          while (tapIndexFieldListIterator.hasNext()) {
            TapIndexField tapIndexField = tapIndexFieldListIterator.next();
            String fieldName = tapIndexField.getName();
            processField(tableId, fieldName, (newFieldName) -> {
              if (StringUtils.isEmpty(newFieldName)) {
                tapIndexFieldListIterator.remove();
              }
              if (!StringUtils.equals(fieldName, newFieldName)) {
                tapIndexField.setName(newFieldName);
              }
            });
          }
          if (CollectionUtils.isEmpty(indexFields)) {
            indexIterator.remove();
          }
        }
        if (CollectionUtils.isNotEmpty(indexList)) {
          processedEvent.set(tapdataEvent);
        }

      } else {
        //no processing required
        processedEvent.set(tapdataEvent);
      }
    }

    if (processedEvent.get() != null) {
      consumer.accept(processedEvent.get(), getProcessResult(tableId));
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("The event does not need to continue to be processed {}", tapdataEvent);
      }
    }
  }

  private void processField(TapdataEvent tapdataEvent, AtomicReference<TapdataEvent> processedEvent,
                            String tableName, String fieldName, Consumer<String> renameFieldNameConsumer) {

    processField(tableName, fieldName, (newFieldName) -> {
      if (StringUtils.isEmpty(newFieldName)) {
        return;
      }
      if (!StringUtils.equals(fieldName, newFieldName)) {
        renameFieldNameConsumer.accept(newFieldName);
      }
      processedEvent.set(tapdataEvent);
    });
  }

  private void processField(String tableName, String fieldName, Consumer<String> renameFieldNameConsumer) {
    Map<String, FieldInfo> fieldInfoMap = this.tableFieldsMappingMap.get(tableName);
    if (MapUtils.isNotEmpty(fieldInfoMap)) {
      FieldInfo fieldInfo = fieldInfoMap.get(fieldName);
      if (fieldInfo != null) {
        if (fieldInfo.getIsShow() != null && !fieldInfo.getIsShow()) {
          renameFieldNameConsumer.accept(null);
        } else {
          renameFieldNameConsumer.accept(fieldInfo.getTargetFieldName());
        }
      }
    }
    renameFieldNameConsumer.accept(fieldName);
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
