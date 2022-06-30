package io.tapdata.entity.conversion;

import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;

import java.util.LinkedHashMap;

public interface TargetTypesGenerator {
    TapResult<LinkedHashMap<String, TapField>> convert(LinkedHashMap<String, TapField> sourceFields, DefaultExpressionMatchingMap targetMatchingMap, TapCodecsFilterManager targetCodecFilterManager);
}
