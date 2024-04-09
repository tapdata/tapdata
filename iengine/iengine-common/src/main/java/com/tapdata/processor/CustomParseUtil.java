package com.tapdata.processor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import org.apache.commons.collections4.MapUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CustomParseUtil {
    public static final String OP_DML_TYPE = "dml";
    public static final String OP_DDL_TYPE = "ddl";
    public static final Map<Integer, Function<Map<String, Object>, TapBaseEvent>> HANDLER_MAP = new HashMap<>();
    public static final Map<String, Function<Map<String, Object>, TapBaseEvent>> HANDLER_MAP_DML = new HashMap<>();

    static {

        HANDLER_MAP.put(209, (record) -> {
            TapNewFieldEvent tapNewFieldEvent = new TapNewFieldEvent();
            List<JSONObject> newFields = (List<JSONObject>) record.get("newFields");
            List<TapField> newFieldList = newFields.stream().map(newFieldMap -> {
//                TapField tapField = new TapField();
//                tapField.setName(newFieldMap.get("newFieldName"));
//                Class<? extends TapType> newFieldType = TapType.getTapTypeClass(newFieldMap.get("newFieldType"));
//                TapType tapType = JavaTypesToTapTypes.toTapType(newFieldType);
//                tapField.setTapType(tapType);
//                TapField tapField=new TapField();
//                tapField.setName(newFieldMap.get("name"));
//                tapField.
//                MapUti
                TapField tapField1 = null;
                try {
                    tapField1= JSON.parseObject(newFieldMap.toJSONString(),TapField.class);
                } catch (Exception e){

                }
                return tapField1;
            }).collect(Collectors.toList());
            tapNewFieldEvent.setTime(System.currentTimeMillis());
            tapNewFieldEvent.setReferenceTime(System.currentTimeMillis());
            tapNewFieldEvent.setNewFields(newFieldList);
            tapNewFieldEvent.setTableId(MapUtils.getString(record, "tableId"));
            return tapNewFieldEvent;
        });
        HANDLER_MAP.put(202, (record) -> {
            TapAlterFieldNameEvent tapAlterFieldNameEvent = new TapAlterFieldNameEvent();
            tapAlterFieldNameEvent.setReferenceTime(System.currentTimeMillis());
            tapAlterFieldNameEvent.setTime(System.currentTimeMillis());
            tapAlterFieldNameEvent.setTableId(MapUtils.getString(record, "tableId"));
            Map nameChange = MapUtils.getMap(record, "nameChange");
            String after = MapUtils.getString(nameChange, "after");
            String before = MapUtils.getString(nameChange, "before");
            ValueChange valueChange = new ValueChange();
            valueChange.setAfter(after);
            valueChange.setBefore(before);
            tapAlterFieldNameEvent.setNameChange(valueChange);
            return tapAlterFieldNameEvent;
        });
        HANDLER_MAP.put(207, (record) -> {
            TapDropFieldEvent tapDropFieldEvent = new TapDropFieldEvent();
            tapDropFieldEvent.setReferenceTime(System.currentTimeMillis());
            tapDropFieldEvent.setTime(System.currentTimeMillis());
            tapDropFieldEvent.setTableId(MapUtils.getString(record, "tableId"));
            String fieldName = MapUtils.getString(record, "fieldName");
            tapDropFieldEvent.setFieldName(fieldName);
            return tapDropFieldEvent;
        });
        HANDLER_MAP.put(205, (record) -> {
            TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent = new TapAlterFieldAttributesEvent();
            return tapAlterFieldAttributesEvent;
        });


        HANDLER_MAP_DML.put("insert", (record) -> {
            TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
            Map<String, Object> after = (Map<String, Object>) MapUtils.getMap(record, "after");
            tapInsertRecordEvent.setAfter(after);
            tapInsertRecordEvent.referenceTime(System.currentTimeMillis());
            return tapInsertRecordEvent;
        });
        HANDLER_MAP_DML.put("update",(record)->{
            TapUpdateRecordEvent tapUpdateRecordEvent=new TapUpdateRecordEvent();
            Map<String, Object> after = (Map<String, Object>) MapUtils.getMap(record, "after");
            tapUpdateRecordEvent.setAfter(after);
            Map<String,Object> before =(Map<String, Object>) MapUtils.getMap(record, "before");
            tapUpdateRecordEvent.setBefore(before);
            tapUpdateRecordEvent.referenceTime(System.currentTimeMillis());
            return tapUpdateRecordEvent;
        });
        HANDLER_MAP_DML.put("delete",(record)->{
            TapDeleteRecordEvent tapDeleteRecordEvent=new TapDeleteRecordEvent();
            Map<String,Object> before =(Map<String, Object>) MapUtils.getMap(record, "before");
            tapDeleteRecordEvent.setBefore(before);
            tapDeleteRecordEvent.referenceTime(System.currentTimeMillis());
            return tapDeleteRecordEvent;
        });

    }

    public static TapBaseEvent applyCustomParse(Map<String, Object> record) {
        TapBaseEvent tapBaseEvent = null;
        String op = MapUtils.getString(record, "op", "invalid");
        if (OP_DDL_TYPE.equals(op)) {
            Integer type = MapUtils.getInteger(record, "type");
            Function<Map<String, Object>, TapBaseEvent> mapTapBaseEventFunction = HANDLER_MAP.get(type);
            return mapTapBaseEventFunction.apply(record);
        } else {
            if (HANDLER_MAP_DML.containsKey(op)) {
                return HANDLER_MAP_DML.get(op).apply(record);
            } else {
                throw new RuntimeException("op type not support");
            }
        }
    }
}
