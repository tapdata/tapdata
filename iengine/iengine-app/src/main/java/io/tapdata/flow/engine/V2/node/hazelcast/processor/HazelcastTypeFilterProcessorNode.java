package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import cn.hutool.core.bean.BeanUtil;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.FieldModTypeFilterNode;
import com.tapdata.tm.commons.dag.process.MigrateTypeFilterProcessorNode;
import com.tapdata.tm.commons.dag.vo.FieldInfo;
import com.tapdata.tm.commons.util.RemoveBracketsUtil;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class HazelcastTypeFilterProcessorNode extends HazelcastProcessorBaseNode {

    private static final Logger logger = LogManager.getLogger(HazelcastTypeFilterProcessorNode.class);

    private final List<String> filterTypes;

    private final Map<String, Map<String, FieldInfo>> fieldTypeFilterMap;
    public HazelcastTypeFilterProcessorNode(ProcessorBaseContext processorBaseContext) {
        super(processorBaseContext);
        Node node = getNode();
        if (node instanceof FieldModTypeFilterNode) {
            FieldModTypeFilterNode filterProces = (FieldModTypeFilterNode) getNode();
            this.filterTypes = filterProces.getFilterTypes();
            this.fieldTypeFilterMap = filterProces.getFieldTypeFilterMap();
        } else if (node instanceof MigrateTypeFilterProcessorNode) {
            MigrateTypeFilterProcessorNode filterProces = (MigrateTypeFilterProcessorNode) getNode();
            this.filterTypes = filterProces.getFilterTypes();
            this.fieldTypeFilterMap = filterProces.getFieldTypeFilterMap();
        } else {
            this.filterTypes = new ArrayList<>();
            this.fieldTypeFilterMap = new HashMap<>();
        }
    }

    @Override
    protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
        TapEvent tapEvent = tapdataEvent.getTapEvent();
        if (!(tapEvent instanceof TapBaseEvent)) {
            consumer.accept(tapdataEvent, null);
        }
        String tableId = TapEventUtil.getTableId(tapEvent);
        AtomicReference<TapdataEvent> processedEvent = new AtomicReference<>();
        if (tapEvent instanceof TapRecordEvent) {
            //dml event
            Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
            Map<String, Object> before = TapEventUtil.getBefore(tapEvent);
            TapEventUtil.setAfter(tapEvent, processFields(after, tableId));
            TapEventUtil.setBefore(tapEvent, processFields(before, tableId));

            processedEvent.set(tapdataEvent);

        }else {
            boolean validEvent = true;
            if (tapEvent instanceof TapNewFieldEvent) {
                TapNewFieldEvent param = (TapNewFieldEvent) tapEvent;
                param.getNewFields().removeIf(tapField -> {
                    FieldInfo fieldInfo = new FieldInfo(tapField.getName(),null,false,tapField.getDataType());
                    Map<String,FieldInfo> map = fieldTypeFilterMap.get(tableId);
                    map.put(tapField.getName(),fieldInfo);
                    return filterTypes.contains(RemoveBracketsUtil.removeBrackets(tapField.getDataType()));
                });
                validEvent = !param.getNewFields().isEmpty();
            }else if (tapEvent instanceof TapAlterFieldAttributesEvent) {
                TapAlterFieldAttributesEvent param = (TapAlterFieldAttributesEvent) tapEvent;
                boolean drop = filterTypes.contains(RemoveBracketsUtil.removeBrackets(param.getDataTypeChange().getAfter()));
                boolean add =  fieldTypeFilterMap.get(tableId).containsKey(param.getFieldName()) &&
                        !filterTypes.contains(RemoveBracketsUtil.removeBrackets(param.getDataTypeChange().getAfter()));
                if(drop){
                    FieldInfo fieldInfo = new FieldInfo(param.getFieldName(),null,false,param.getDataTypeChange().getAfter());
                    Map<String,FieldInfo> map = fieldTypeFilterMap.get(tableId);
                    map.put(param.getFieldName(),fieldInfo);
                    TapDropFieldEvent tapDropFieldEvent = new TapDropFieldEvent();
                    BeanUtil.copyProperties(param,tapDropFieldEvent);
                    tapDropFieldEvent.setFieldName(param.getFieldName());
                    tapDropFieldEvent.setTableId(param.getTableId());
                    tapdataEvent.setTapEvent(tapDropFieldEvent);
                }else if(add){
                    fieldTypeFilterMap.get(tableId).remove(param.getFieldName());
                    TapNewFieldEvent tapNewFieldEvent = new TapNewFieldEvent();
                    BeanUtil.copyProperties(param,tapNewFieldEvent);
                    List<TapField> newFields = new ArrayList<>();
                    TapField tapField = new TapField();
                    tapField.setName(param.getFieldName());
                    tapField.setDataType(param.getDataTypeChange().getAfter());
                    Optional.ofNullable(param.getCheckChange()).ifPresent(t -> tapField.setCheck(t.getAfter()));
                    Optional.ofNullable(param.getConstraintChange()).ifPresent(t -> tapField.setConstraint(t.getAfter()));
                    Optional.ofNullable(param.getCommentChange()).ifPresent(t -> tapField.setComment(t.getAfter()));
                    Optional.ofNullable(param.getDefaultChange()).ifPresent(t -> tapField.setDefaultValue(t.getAfter()));
                     newFields.add(tapField);
                    tapNewFieldEvent.setNewFields(newFields);
                    tapdataEvent.setTapEvent(tapNewFieldEvent);
                }
            }
            if(validEvent){
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

    private Map<String, Object> processFields(Map<String, Object> map, String tableName) {
        if (MapUtils.isEmpty(map)) {
            return map;
        }
        if (StringUtils.isEmpty(tableName)) {
            return map;
        }
        Map<String, FieldInfo> fieldsMappingMap = fieldTypeFilterMap.get(tableName);
        if (MapUtils.isEmpty(fieldsMappingMap)) {
            return map;
        }

        for (Map.Entry<String, FieldInfo> entry : fieldsMappingMap.entrySet()) {
            if (MapUtil.containsKey(map, entry.getKey())) {
                MapUtil.removeValueByKey(map, entry.getKey());
            }
        }
        return map;
    }
}
