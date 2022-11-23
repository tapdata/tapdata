package io.tapdata.util;

import io.tapdata.constant.JsonType;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public class JsonSchemaParser {

    public void parse(TapTable table, Map<String, Object> obj) {
        this.parse(table, null, null, obj, true);
    }

    private String fieldNameConcat(String p, String c) {
        return EmptyKit.isEmpty(p) ? c : String.format("%s.%s", p, c);
    }

    private void parse(TapTable table, String parentFieldName, String fieldName, Object obj, boolean isRoot) {
        JsonType jsonType = JsonType.of(obj);
        if (jsonType != null) {
            switch (jsonType) {
                case OBJECT: {
                    if (!EmptyKit.isEmpty(fieldName) && !isRoot) {
                        table.add(this.buildField(parentFieldName, fieldName, JsonType.OBJECT));
                    }
                    Map<String, Object> map = (Map<String, Object>) obj;
                    for (Map.Entry<String, Object> e : map.entrySet()) {
                        this.parse(table, fieldNameConcat(parentFieldName, fieldName), e.getKey(), e.getValue(), false);
                    }
                    break;
                }
                case ARRAY: {
                    Object elem = null;
                    if (obj instanceof Collection) {
                        Iterator iterator = ((Collection) obj).iterator();
                        if (iterator.hasNext()) {
                            elem = iterator.next();
                        }
                    } else {
                        Object[] array = (Object[]) obj;
                        if (array.length > 0) {
                            elem = array[0];
                        }
                    }
                    JsonType subJsonType = JsonType.of(elem);
                    boolean isParseElem = subJsonType == JsonType.OBJECT;
                    table.add(this.buildField(parentFieldName, fieldName, JsonType.ARRAY));
                    if (isParseElem) {
                        this.parse(table, parentFieldName, fieldName, elem, true);
                    }
                    break;
                }
                case NUMBER:
                case STRING:
                case BOOLEAN:
                case INTEGER:
                case TEXT:
                    table.add(this.buildField(parentFieldName, fieldName, jsonType));
                    break;
                case NULL:
                    // not to parse NULL value
                    break;
                default:
                    table.add(this.buildField(parentFieldName, fieldName, JsonType.STRING));
            }
        }
    }

    private TapField buildField(String parentFieldName, String fieldName, JsonType jsonType) {
        final String name = EmptyKit.isEmpty(parentFieldName) ? fieldName : fieldNameConcat(parentFieldName, fieldName);
        TapField field = new TapField();
        field.setName(name);
        field.setNullable(true);
        field.setDataType(jsonType.name());
        return field;
    }

}
