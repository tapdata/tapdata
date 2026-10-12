package io.tapdata.pdk.cli.support;

import io.tapdata.entity.mapping.type.*;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.*;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;

import java.util.HashSet;
import java.util.Set;

import static io.tapdata.entity.simplify.TapSimplify.field;

public class DataTypesHandler {
    public static final String FIELD_PREFIX = "prefix_";
    public static final String FIELD_EXACT = "exact_";
    public BiClassHandlers<TapMapping, TableExpressionWrapper, Void> biClassHandlers;
    public BiClassHandlers<TapMapping, String, String> dataNameClassHandlers;

    public void fillTestFields(TapTable tapTable, String expression, TapMapping tapMapping) {
        biClassHandlers.handle(tapMapping, new TableExpressionWrapper(tapTable, expression));
    }

    public static DataTypesHandler create() {
        return new DataTypesHandler();
    }

    public BiClassHandlers<TapMapping, TableExpressionWrapper, Void> biClassHandlers() {
        return this.biClassHandlers;
    }

    public BiClassHandlers<TapMapping, String, String> dataNameClassHandlers() {
        return this.dataNameClassHandlers;
    }

    private DataTypesHandler() {
        this.initBiClassHandlers();
        this.initDataNameClassHandlers();
    }

    private void initDataNameClassHandlers() {
        if (dataNameClassHandlers == null) {
            dataNameClassHandlers = new BiClassHandlers<>();
            dataNameClassHandlers.register(TapStringMapping.class, (tapMapping, expression) -> this.generateDataTypeName(tapMapping, expression, new TapString()));
            dataNameClassHandlers.register(TapYearMapping.class, (tapMapping, expression) -> this.generateDataTypeName(tapMapping, expression, new TapYear()));
            dataNameClassHandlers.register(TapNumberMapping.class, (tapMapping, expression) -> this.generateDataTypeName(tapMapping, expression, new TapNumber()));
            dataNameClassHandlers.register(TapRawMapping.class, (tapMapping, expression) -> this.generateDataTypeName(tapMapping, expression, new TapRaw()));
            dataNameClassHandlers.register(TapBooleanMapping.class, (tapMapping, expression) -> this.generateDataTypeName(tapMapping, expression, new TapBoolean()));
            dataNameClassHandlers.register(TapDateMapping.class, (tapMapping, expression) -> this.generateDataTypeName(tapMapping, expression, new TapDate()));
            dataNameClassHandlers.register(TapDateTimeMapping.class, (tapMapping, expression) -> this.generateDataTypeName(tapMapping, expression, new TapDateTime()));
            dataNameClassHandlers.register(TapTimeMapping.class, (tapMapping, expression) -> this.generateDataTypeName(tapMapping, expression, new TapTime()));
            dataNameClassHandlers.register(TapFloatMapping.class, (tapMapping, expression) -> this.generateDataTypeName(tapMapping, expression, new TapFloat()));
            dataNameClassHandlers.register(TapDoubleMapping.class, (tapMapping, expression) -> this.generateDataTypeName(tapMapping, expression, new TapDouble()));
            dataNameClassHandlers.register(TapCoefficientFloatMapping.class, (tapMapping, expression) -> this.generateDataTypeName(tapMapping, expression, new TapFloat()));
            dataNameClassHandlers.register(TapMapMapping.class, (tapMapping, expression) -> this.generateDataTypeName(tapMapping, expression, new TapMap()));
            dataNameClassHandlers.register(TapArrayMapping.class, (tapMapping, expression) -> this.generateDataTypeName(tapMapping, expression, new TapArray()));
            dataNameClassHandlers.register(TapBinaryMapping.class, (tapMapping, expression) -> this.generateDataTypeName(tapMapping, expression, new TapBinary()));
        }
    }

    private void initBiClassHandlers() {
        if (biClassHandlers == null) {
            biClassHandlers = new BiClassHandlers<>();
            biClassHandlers.register(TapStringMapping.class, this::handleStringMapping);
            biClassHandlers.register(TapYearMapping.class, this::handleYearMapping);
            biClassHandlers.register(TapNumberMapping.class, this::handleNumberMapping);
            biClassHandlers.register(TapRawMapping.class, this::handleRawMapping);
            biClassHandlers.register(TapBooleanMapping.class, this::handleBooleanMapping);
            biClassHandlers.register(TapDateMapping.class, this::handleDateMapping);
            biClassHandlers.register(TapDateTimeMapping.class, this::handleDateTimeMapping);
            biClassHandlers.register(TapTimeMapping.class, this::handleTimeMapping);
            biClassHandlers.register(TapFloatMapping.class, this::handleFloatMapping);
            biClassHandlers.register(TapDoubleMapping.class, this::handleDoubleMapping);
            biClassHandlers.register(TapCoefficientFloatMapping.class, this::handleCoefficientFloatMapping);
            biClassHandlers.register(TapMapMapping.class, this::handleMapMapping);
            biClassHandlers.register(TapArrayMapping.class, this::handleArrayMapping);
            biClassHandlers.register(TapBinaryMapping.class, this::handleBinaryMapping);
        }
    }

    private String generateDataTypeName(TapMapping tapMapping, String expression, TapType tapType) {
        if (tapMapping.getName() != null)
            return tapMapping.getName();
        TapResult<String> result = floatingPointFromTapType(tapMapping, expression, tapType);
        if (result != null)
            return result.getData();
        return null;
    }

    private TapResult<String> floatingPointFromTapType(TapMapping tapMapping, String expression, TapType tapType) {
        TapResult<String> result = tapMapping.fromTapType(expression, tapType);
        if (result == null || result.getData() == null
                || (!(tapMapping instanceof TapFloatMapping) && !(tapMapping instanceof TapDoubleMapping))) {
            return result;
        }

        TapType resolvedType = tapMapping.toTapType(expression, null);
        if (!(resolvedType instanceof TapNumber)) {
            return result;
        }
        result.data(normalizeFloatingPointExpression(result.getData(), (TapNumber) resolvedType));
        return result;
    }

    private String normalizeFloatingPointExpression(String expression, TapNumber type) {
        StringBuilder normalized = new StringBuilder();
        int start = 0;
        while (start < expression.length()) {
            int open = expression.indexOf('[', start);
            if (open < 0) {
                normalized.append(expression.substring(start));
                break;
            }
            normalized.append(expression, start, open);

            int close = expression.indexOf(']', open + 1);
            if (close < 0) {
                normalized.append(expression.substring(open + 1));
                break;
            }

            String content = expression.substring(open + 1, close);
            boolean unresolvedVariable = content.contains("$precision") && type.getPrecision() == null
                    || content.contains("$scale") && type.getScale() == null;
            if (!unresolvedVariable) {
                String resolvedContent = replaceFloatingPointVariables(content, type);
                if (isFloatingPointModifier(resolvedContent)) {
                    if (normalized.length() > 0 && !Character.isWhitespace(normalized.charAt(normalized.length() - 1))) {
                        normalized.append(' ');
                    }
                }
                normalized.append(resolvedContent);
            }
            start = close + 1;
        }

        String result = replaceFloatingPointVariables(normalized.toString(), type);
        return result;
    }

    private String replaceFloatingPointVariables(String expression, TapNumber type) {
        String result = expression;
        if (result.contains("$precision")) {
            result = result.replace("$precision", type.getPrecision() == null ? "1" : String.valueOf(type.getPrecision()));
        }
        if (result.contains("$scale")) {
            result = result.replace("$scale", type.getScale() == null ? "0" : String.valueOf(type.getScale()));
        }
        return result;
    }

    private boolean isFloatingPointModifier(String expression) {
        return "unsigned".equalsIgnoreCase(expression.trim())
                || "zerofill".equalsIgnoreCase(expression.trim());
    }

    private Void handleBinaryMapping(TapMapping tapMapping, TableExpressionWrapper tableExpressionWrapper) {
        HashSet<String> fields = new HashSet<>();
        TapBinaryMapping binaryMapping = (TapBinaryMapping) tapMapping;
        boolean needEmpty = true;
        if (binaryMapping.getBytes() != null) {
            needEmpty = false;
            TapResult<String> result;
            result = binaryMapping.fromTapType(tableExpressionWrapper.expression(), new TapBinary().bytes(binaryMapping.getBytes()));
            if (result != null && result.getResult() != TapResult.RESULT_FAILED)
                fields.add(result.getData());

            result = binaryMapping.fromTapType(tableExpressionWrapper.expression(), new TapBinary().bytes(1L));
            if (result != null && result.getResult() != TapResult.RESULT_FAILED)
                fields.add(result.getData());
        }
        if (needEmpty) {
            TapResult<String> result = binaryMapping.fromTapType(tableExpressionWrapper.expression(), new TapBinary());
            if (result != null && result.getResult() != TapResult.RESULT_FAILED)
                fields.add(result.getData());
        }

        for (String field : fields) {
            tableExpressionWrapper.table().add(field(FIELD_PREFIX + field, field));
        }
        return null;
    }

    private Void handleArrayMapping(TapMapping tapMapping, TableExpressionWrapper tableExpressionWrapper) {
        TapArrayMapping arrayMapping = (TapArrayMapping) tapMapping;
        TapResult<String> result = arrayMapping.fromTapType(tableExpressionWrapper.expression(), new TapArray());
        String dataType = result.getData();
        tableExpressionWrapper.table().add(field(FIELD_PREFIX + dataType, dataType));
        return null;
    }

    private Void handleMapMapping(TapMapping tapMapping, TableExpressionWrapper tableExpressionWrapper) {
        TapMapMapping tapMapMapping = (TapMapMapping) tapMapping;
        TapResult<String> result = tapMapMapping.fromTapType(tableExpressionWrapper.expression(), new TapMap());
        String dataType = result.getData();
        tableExpressionWrapper.table().add(field(FIELD_PREFIX + dataType, dataType));
        return null;
    }

    private Void handleTimeMapping(TapMapping tapMapping, TableExpressionWrapper tableExpressionWrapper) {
        TapTimeMapping tapMapMapping = (TapTimeMapping) tapMapping;
        TapResult<String> result = tapMapMapping.fromTapType(tableExpressionWrapper.expression(), new TapTime());
        String dataType = result.getData();
        tableExpressionWrapper.table().add(field(FIELD_PREFIX + dataType, dataType));
        return null;
    }

    private Void handleFloatMapping(TapMapping tapMapping, TableExpressionWrapper tableExpressionWrapper) {
        TapFloatMapping floatMapping = (TapFloatMapping) tapMapping;
        addFloatingPointField(floatingPointFromTapType(floatMapping, tableExpressionWrapper.expression(), new TapFloat()), tableExpressionWrapper);
        return null;
    }

    private Void handleDoubleMapping(TapMapping tapMapping, TableExpressionWrapper tableExpressionWrapper) {
        TapDoubleMapping doubleMapping = (TapDoubleMapping) tapMapping;
        addFloatingPointField(floatingPointFromTapType(doubleMapping, tableExpressionWrapper.expression(), new TapDouble()), tableExpressionWrapper);
        return null;
    }

    private Void handleCoefficientFloatMapping(TapMapping tapMapping, TableExpressionWrapper tableExpressionWrapper) {
        TapCoefficientFloatMapping coefficientMapping = (TapCoefficientFloatMapping) tapMapping;
        addFloatingPointField(coefficientMapping.fromTapType(tableExpressionWrapper.expression(), new TapFloat()), tableExpressionWrapper);
        addFloatingPointField(coefficientMapping.fromTapType(tableExpressionWrapper.expression(), new TapDouble()), tableExpressionWrapper);
        return null;
    }

    private void addFloatingPointField(TapResult<String> result, TableExpressionWrapper tableExpressionWrapper) {
        if (result != null && result.getResult() != TapResult.RESULT_FAILED && result.getData() != null) {
            String dataType = result.getData();
            tableExpressionWrapper.table().add(field(FIELD_PREFIX + dataType, dataType));
        }
    }

    private Void handleDateTimeMapping(TapMapping tapMapping, TableExpressionWrapper tableExpressionWrapper) {
        Set<String> fieldDataTypes = new HashSet<>();
        TapDateTimeMapping dateTimeMapping = (TapDateTimeMapping) tapMapping;

        boolean needEmpty = true;
        if (dateTimeMapping.getMinFraction() != null && dateTimeMapping.getMaxFraction() != null) {
            needEmpty = false;
            TapResult<String> result;

            result = dateTimeMapping.fromTapType(tableExpressionWrapper.expression(), new TapDateTime().fraction(dateTimeMapping.getMinFraction()));
            fieldDataTypes.add(result.getData());

            result = dateTimeMapping.fromTapType(tableExpressionWrapper.expression(), new TapDateTime().fraction(dateTimeMapping.getMaxFraction()));
            fieldDataTypes.add(result.getData());
        }

        if (dateTimeMapping.getWithTimeZone() != null && dateTimeMapping.getWithTimeZone()) {
            TapResult<String> result;

            result = dateTimeMapping.fromTapType(tableExpressionWrapper.expression(), new TapDateTime().withTimeZone(true));
            fieldDataTypes.add(result.getData());
        }

        if (needEmpty) {
            TapResult<String> result = dateTimeMapping.fromTapType(tableExpressionWrapper.expression(), new TapDateTime());
            fieldDataTypes.add(result.getData());
        }

        for (String fieldDataType : fieldDataTypes) {
            tableExpressionWrapper.table().add(field(FIELD_PREFIX + fieldDataType, fieldDataType));
        }
        return null;
    }

    private Void handleDateMapping(TapMapping tapMapping, TableExpressionWrapper tableExpressionWrapper) {
        Set<String> fieldDataTypes = new HashSet<>();
        TapDateMapping dateMapping = (TapDateMapping) tapMapping;

        if (dateMapping.getWithTimeZone() != null && dateMapping.getWithTimeZone()) {
            TapResult<String> result;

            result = dateMapping.fromTapType(tableExpressionWrapper.expression(), new TapDate().withTimeZone(true));
            fieldDataTypes.add(result.getData());

            result = dateMapping.fromTapType(tableExpressionWrapper.expression(), new TapDate().withTimeZone(false));
            fieldDataTypes.add(result.getData());
        }
        TapResult<String> result = dateMapping.fromTapType(tableExpressionWrapper.expression(), new TapDate());
        fieldDataTypes.add(result.getData());


        for (String fieldDataType : fieldDataTypes) {
            tableExpressionWrapper.table().add(field(FIELD_PREFIX + fieldDataType, fieldDataType));
        }

        return null;
    }

    private Void handleBooleanMapping(TapMapping tapMapping, TableExpressionWrapper tableExpressionWrapper) {
        TapBooleanMapping rawMapping = (TapBooleanMapping) tapMapping;
        TapResult<String> result = rawMapping.fromTapType(tableExpressionWrapper.expression(), new TapBoolean());
        String dataType = result.getData();
        tableExpressionWrapper.table().add(field(FIELD_PREFIX + dataType, dataType));
        return null;
    }

    private Void handleRawMapping(TapMapping tapMapping, TableExpressionWrapper tableExpressionWrapper) {
        TapRawMapping rawMapping = (TapRawMapping) tapMapping;
        TapResult<String> result = rawMapping.fromTapType(tableExpressionWrapper.expression(), new TapRaw());
        String dataType = result.getData();
        tableExpressionWrapper.table().add(field(FIELD_PREFIX + dataType, dataType));
        return null;
    }

    private Void handleNumberMapping(TapMapping tapMapping, TableExpressionWrapper tableExpressionWrapper) {
        HashSet<String> fields = new HashSet<>();
        TapNumberMapping numberMapping = (TapNumberMapping) tapMapping;
        boolean needEmpty = true;
        if (numberMapping.getBit() != null) {
            needEmpty = false;
            TapResult<String> result;
            result = numberMapping.fromTapType(tableExpressionWrapper.expression(), new TapNumber().bit(numberMapping.getBit()));
            if (result != null && result.getResult() != TapResult.RESULT_FAILED)
                fields.add(result.getData());
            result = numberMapping.fromTapType(tableExpressionWrapper.expression(), new TapNumber().bit(1));
            if (result != null && result.getResult() != TapResult.RESULT_FAILED)
                fields.add(result.getData());
            if (numberMapping.getUnsigned() != null) {
                result = numberMapping.fromTapType(tableExpressionWrapper.expression(), new TapNumber().bit(numberMapping.getBit()).unsigned(true));
                if (result != null && result.getResult() != TapResult.RESULT_FAILED)
                    fields.add(result.getData());

                result = numberMapping.fromTapType(tableExpressionWrapper.expression(), new TapNumber().bit(numberMapping.getBit()).unsigned(false));
                if (result != null && result.getResult() != TapResult.RESULT_FAILED)
                    fields.add(result.getData());
            }
        }
        if (numberMapping.getMaxPrecision() != null &&
                numberMapping.getMinPrecision() != null &&
                numberMapping.getMaxScale() != null &&
                numberMapping.getMinScale() != null
        ) {
            needEmpty = false;
            TapResult<String> result;
            result = numberMapping.fromTapType(tableExpressionWrapper.expression(), new TapNumber().precision(numberMapping.getMaxPrecision()).scale(numberMapping.getMinScale()));
            if (result != null && result.getResult() != TapResult.RESULT_FAILED)
                fields.add(result.getData());
            if (numberMapping.getMaxScale() / 2 > numberMapping.getMinScale()) {
                result = numberMapping.fromTapType(tableExpressionWrapper.expression(), new TapNumber().precision(numberMapping.getMaxPrecision()).scale(numberMapping.getMaxScale() / 2));
                if (result != null && result.getResult() != TapResult.RESULT_FAILED)
                    fields.add(result.getData());
            }
            if (numberMapping.getUnsigned() != null) {
                result = numberMapping.fromTapType(tableExpressionWrapper.expression(), new TapNumber().precision(numberMapping.getMaxPrecision()).scale(numberMapping.getMinScale()).unsigned(true));
                if (result != null && result.getResult() != TapResult.RESULT_FAILED)
                    fields.add(result.getData());

                result = numberMapping.fromTapType(tableExpressionWrapper.expression(), new TapNumber().precision(numberMapping.getMaxPrecision()).scale(numberMapping.getMinScale()).unsigned(false));
                if (result != null && result.getResult() != TapResult.RESULT_FAILED)
                    fields.add(result.getData());
            }
        }
        if (needEmpty) {
            TapResult<String> result = numberMapping.fromTapType(tableExpressionWrapper.expression(), new TapNumber());
            if (result != null && result.getResult() != TapResult.RESULT_FAILED)
                fields.add(result.getData());
        }
        for (String field : fields) {
            tableExpressionWrapper.table().add(field(FIELD_PREFIX + field, field));
        }
        return null;
    }

    private Void handleYearMapping(TapMapping tapMapping, TableExpressionWrapper tableExpressionWrapper) {
        TapYearMapping yearMapping = (TapYearMapping) tapMapping;
        TapResult<String> result = yearMapping.fromTapType(tableExpressionWrapper.expression(), new TapYear());
        String dataType = result.getData();
        tableExpressionWrapper.table().add(field(FIELD_PREFIX + dataType, dataType));
        return null;
    }

    private Void handleStringMapping(TapMapping tapMapping, TableExpressionWrapper tableExpressionWrapper) {
        HashSet<String> fieldDataTypes = new HashSet<>();
        TapStringMapping stringMapping = (TapStringMapping) tapMapping;
        if (stringMapping.getBytes() != null) {
            TapResult<String> result;
            result = stringMapping.fromTapType(tableExpressionWrapper.expression(), new TapString().bytes(stringMapping.getBytes()).byteRatio(stringMapping.getByteRatio()));
            if (result != null && result.getResult() != TapResult.RESULT_FAILED)
                fieldDataTypes.add(result.getData());

            result = stringMapping.fromTapType(tableExpressionWrapper.expression(), new TapString().bytes(1L).byteRatio(stringMapping.getByteRatio()));
            if (result != null && result.getResult() != TapResult.RESULT_FAILED)
                fieldDataTypes.add(result.getData());
        } else {
            TapResult<String> result = stringMapping.fromTapType(tableExpressionWrapper.expression(), new TapString());
            if (result != null && result.getResult() != TapResult.RESULT_FAILED)
                fieldDataTypes.add(result.getData());
        }
        for (String fieldDataType : fieldDataTypes) {
            tableExpressionWrapper.table().add(field(FIELD_PREFIX + fieldDataType, fieldDataType));
        }
        return null;
    }
}
