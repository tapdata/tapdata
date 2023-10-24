
package io.tapdata.entity.mapping;

import io.tapdata.entity.mapping.type.TapMapping;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.TypeHolder;

import java.util.Map;

public class DefaultExpressionMatchingMap extends ExpressionMatchingMap<DataMap> {
    public DefaultExpressionMatchingMap(Map<String, DataMap> map) {
        super(map);
        this.setValueFilter(defaultMap -> {
            TapMapping tapMapping = (TapMapping) defaultMap.get(TapMapping.FIELD_TYPE_MAPPING);
            if(tapMapping == null) {
                defaultMap.put(TapMapping.FIELD_TYPE_MAPPING, TapMapping.build(defaultMap));
            }
        });
    }

    public static DefaultExpressionMatchingMap map(String json) {
        return new DefaultExpressionMatchingMap(InstanceFactory.instance(JsonParser.class).fromJson(json, new TypeHolder<Map<String, DataMap>>(){}));
    }

    public static DefaultExpressionMatchingMap map(Map<String, DataMap> map) {
        return new DefaultExpressionMatchingMap(map);
    }
}
