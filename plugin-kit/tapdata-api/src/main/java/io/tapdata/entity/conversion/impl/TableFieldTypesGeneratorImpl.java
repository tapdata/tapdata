package io.tapdata.entity.conversion.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.mapping.TypeExprResult;
import io.tapdata.entity.mapping.type.TapMapping;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DataMap;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static io.tapdata.entity.simplify.TapSimplify.tapRaw;

@Implementation(value = TableFieldTypesGenerator.class, buildNumber = 0)
public class TableFieldTypesGeneratorImpl implements TableFieldTypesGenerator {
    private static final String TAG = TableFieldTypesGenerator.class.getSimpleName();
    @Override
    public void autoFill(LinkedHashMap<String, TapField> nameFieldMap, DefaultExpressionMatchingMap expressionMatchingMap) {
        for(Map.Entry<String, TapField> entry : nameFieldMap.entrySet()) {
            if(entry.getValue().getDataType() != null) {
                Set<String> ignoreExpressionSet = null;
                boolean needRetry;

                do {
                    needRetry = false;
                    TypeExprResult<DataMap> result = expressionMatchingMap.get(entry.getValue().getDataType(), ignoreExpressionSet);
                    if(result != null) {
                        TapMapping tapMapping = (TapMapping) result.getValue().get(TapMapping.FIELD_TYPE_MAPPING);
                        if(tapMapping != null) {
                            try {
                                entry.getValue().setTapType(tapMapping.toTapType(entry.getValue().getDataType(), result.getParams()));
                            } catch (CoreException e) {
                                TapLogger.warn(TAG, "Ignore {} to try others, because params {} error {}", result.getExpression(), result.getParams(), e.getMessage());
                                if(ignoreExpressionSet == null)
                                    ignoreExpressionSet = new HashSet<>();
                                ignoreExpressionSet.add(result.getExpression());
                                needRetry = true;
                            }
                        }
                    } else {
                        if(entry.getValue().getTapType() == null) {
                            entry.getValue().setTapType(tapRaw());
                            TapLogger.warn(TAG, "Field dataType {} didn't match corresponding TapMapping, TapRaw will be used for this dataType. ", entry.getValue().getDataType());
                        }
                    }
                } while(needRetry);

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
//<<<<<<< HEAD
//            tapField.setTapType(tapRaw());
//            TapLogger.debug(TAG, "Field originType {} didn't match corresponding TapMapping, TapRaw will be used for this dataType. ", tapField.getDataType());
//=======
            if(tapField.getTapType() == null) {
                tapField.setTapType(tapRaw());
                TapLogger.warn(TAG, "Field originType {} didn't match corresponding TapMapping, TapRaw will be used for this dataType. ", tapField.getDataType());
            }
//>>>>>>> develop-v2.9-coding-connector
        }
    }
}
