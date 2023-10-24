package io.tapdata.entity.conversion;

import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;

import java.util.LinkedHashMap;
import java.util.Map;

public interface TargetTypesGenerator {
    TapResult<LinkedHashMap<String, TapField>> convert(LinkedHashMap<String, TapField> sourceFields, DefaultExpressionMatchingMap targetMatchingMap, TapCodecsFilterManager targetCodecFilterManager);
    TapResult<LinkedHashMap<String, TapField>> convert(LinkedHashMap<String, TapField> sourceFields, DefaultExpressionMatchingMap targetMatchingMap, TapCodecsFilterManager targetCodecFilterManager, Map<String, PossibleDataTypes> findPossibleDataTypes);
}
