package com.tapdata.tm.modules.util;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.enums.TableFieldTag;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.module.entity.Path;
import io.tapdata.entity.schema.type.TapArray;
import io.tapdata.entity.schema.type.TapMap;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.simplify.TapSimplify;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/8 15:09 Create
 * @description
 */
public final class FieldTypeUtil {
    // List.of("Array", "Map", "Boolean", "Integer", "Number", "String", "Time", "Date", "DateTime", "Any")
    public static final Map<String, String> FILED_TYPE = Map.of(
            "Array", "{\"type\": 2}",
            "Map", "{\"type\": 4}",
            "Boolean", "{\"type\": 3}",
            "Integer", "{\"bit\":32,\"maxValue\":2147483647,\"minValue\":-2147483648,\"precision\":10,\"type\":8}",
            "Number", "{\"fixed\":false,\"maxValue\":1.7976931348623157E+308,\"minValue\":-1.7976931348623157E+308,\"precision\":255,\"scale\":30,\"type\":8}",
            "String", "{\"type\": 10}",
            "Time", "{\"defaultFraction\":3,\"fraction\":3,\"max\":\"23:59:59.999\",\"min\":\"00:00:00.000\",\"type\":6}",
            "Date", "{\"defaultFraction\":0,\"fraction\":0,\"max\":\"9999-12-31\",\"min\":\"1000-01-01\",\"type\":11}",
            "DateTime", "{\"defaultFraction\":3,\"fraction\":3,\"max\":\"9999-12-31T23:59:59.999Z\",\"min\":\"1000-01-01T00:00:00.001Z\",\"type\":1}",
            "Any", "{\"type\": 7}"
    );
    private FieldTypeUtil() {

    }

    public static void parseTapType(Field field) {
        String tapType = field.getTapType();
        TapType type = TapSimplify.fromJson(tapType, TapType.class);
        if (type == null) {
            field.setSimpleTypeName("Any");
        } else {
            field.setSimpleTypeName(type.getClass().getSimpleName().replace("Tap", ""));
        }
    }

    public static void parseTapType(List<Field> field) {
        if (CollectionUtils.isEmpty(field)) {
            return;
        }
        field.forEach(FieldTypeUtil::parseTapType);
    }

    public static void validCustomWhereIfNeed(ModulesDto modulesDto) {
        if (null == modulesDto) {
            return;
        }
        List<Field> fields = modulesDto.getFields();
        if (CollectionUtils.isNotEmpty(fields)) {
            for (Field field : fields) {
                FieldTypeUtil.genericFieldIfNeed(field);
            }
        }
        List<Path> paths = modulesDto.getPaths();
        if (CollectionUtils.isNotEmpty(paths)) {
            for (Path path : paths) {
                List<Field> fieldsInPath = path.getFields();
                if (CollectionUtils.isNotEmpty(fieldsInPath)) {
                    for (Field field : fieldsInPath) {
                        FieldTypeUtil.genericFieldIfNeed(field);
                    }
                }
            }
        }
    }

    public static void genericFieldIfNeed(Field field) {
        String tag = field.getTag();
        if (StringUtils.isBlank(field.getTapType()) && TableFieldTag.USER_CREATE.getType().equals(tag)) {
            String simpleTypeName = field.getSimpleTypeName();
            if (StringUtils.isBlank(simpleTypeName)) {
                throw new BizException("schema.field.type.empty");
            }
            String tapType = FILED_TYPE.get(simpleTypeName.trim());
            if (null == tapType) {
                throw new BizException("schema.field.type.noSupport", simpleTypeName, TapSimplify.toJson(FILED_TYPE.keySet()));
            }
            field.setTapType(tapType);
        }

        String fieldName = field.getFieldName();
        if (StringUtils.isBlank(fieldName)) {
            throw new BizException("schema.field.name.empty");
        }
        if (StringUtils.isBlank(field.getOriginalFieldName())) {
            field.setOriginalFieldName(fieldName);
        }
    }
}
