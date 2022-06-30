package io.tapdata.entity.conversion.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.mapping.TypeExprResult;
import io.tapdata.entity.mapping.type.TapMapping;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DataMap;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.tapRaw;

@Implementation(value = TableFieldTypesGenerator.class, buildNumber = 0)
public class TableFieldTypesGeneratorImpl implements TableFieldTypesGenerator {
    private static final String TAG = TableFieldTypesGenerator.class.getSimpleName();
    @Override
    public void autoFill(LinkedHashMap<String, TapField> nameFieldMap, DefaultExpressionMatchingMap expressionMatchingMap) {
        for(Map.Entry<String, TapField> entry : nameFieldMap.entrySet()) {
            if(entry.getValue().getDataType() != null) {
                TypeExprResult<DataMap> result = expressionMatchingMap.get(entry.getValue().getDataType());
                if(result != null) {
                    TapMapping tapMapping = (TapMapping) result.getValue().get(TapMapping.FIELD_TYPE_MAPPING);
                    if(tapMapping != null) {
                        entry.getValue().setTapType(tapMapping.toTapType(entry.getValue().getDataType(), result.getParams()));
                    }
                } else {
                    entry.getValue().setTapType(tapRaw());
                    TapLogger.warn(TAG, "Field dataType {} didn't match corresponding TapMapping, TapRaw will be used for this dataType. ", entry.getValue().getDataType());
                }
            }
        }
    }

    @Override
    public void autoFill(TapField tapField, DefaultExpressionMatchingMap expressionMatchingMap) {
        if(null == tapField) return;
        TypeExprResult<DataMap> result = expressionMatchingMap.get(tapField.getDataType());
        if(result != null) {
            TapMapping tapMapping = (TapMapping) result.getValue().get(TapMapping.FIELD_TYPE_MAPPING);
            if(tapMapping != null) {
                tapField.setTapType(tapMapping.toTapType(tapField.getDataType(), result.getParams()));
            }
        } else {
            tapField.setTapType(tapRaw());
            TapLogger.warn(TAG, "Field originType {} didn't match corresponding TapMapping, TapRaw will be used for this dataType. ", tapField.getDataType());
        }
    }
}
