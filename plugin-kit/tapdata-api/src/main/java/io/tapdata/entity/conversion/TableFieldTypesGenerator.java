package io.tapdata.entity.conversion;

import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapField;

import java.util.LinkedHashMap;

public interface TableFieldTypesGenerator {
    void autoFill(LinkedHashMap<String, TapField> fieldMap, DefaultExpressionMatchingMap expressionMatchingMap);

    void autoFill(TapField tapField, DefaultExpressionMatchingMap expressionMatchingMap);
}
