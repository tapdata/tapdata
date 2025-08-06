package com.tapdata.tm.modules.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tapdata.tm.base.exception.BizException;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/8/5 11:58 Create
 * @description 关于Mongo高级查询where内容格式校验
 */
public final class MongoQueryValidator {
    private static final Set<String> MONGO_OPERATORS = Set.of(
            "$eq", "$gt", "$gte", "$in", "$lt", "$lte", "$ne", "$nin",
            "$and", "$nor", "$or",
            "$exists", "$type",
            "$regex",
            "$all", "$elemMatch", "$size",
            "$mod", "$options"
    );
    private static final Pattern TEMPLATE_PATTERN =
            Pattern.compile("^\\{\\{[a-zA-Z_][a-zA-Z0-9_]*\\}\\}$");

    MongoQueryValidator() {
    }

    static String logicalOperator(ObjectNode queryObj) {
        String logicalOperator = null;
        if (queryObj.has("$and")) {
            logicalOperator = "$and";
        } else if (queryObj.has("$or")) {
            logicalOperator = "$or";
        } else if (queryObj.has("$nor")) {
            logicalOperator = "$nor";
        }
        return logicalOperator;
    }

    public static ValidationResult checkWhere(JsonNode query, ValidationContext context) {
        if (context.getCurrentDepth() > context.getMaxDepth()) {
            throw new BizException("module.save.check.where.tooDeep", context.getMaxDepth());
        }
        if (!query.isObject()) {
            throw new BizException("module.save.check.where.notJson");
        }
        ObjectNode queryObj = (ObjectNode) query;
        Set<Map.Entry<String, JsonNode>> fields = queryObj.properties();
        String logicalOperator = logicalOperator(queryObj);
        if (Objects.nonNull(logicalOperator)) {
            if (!context.getAllowedOperators().contains(logicalOperator)) {
                throw new BizException("module.save.check.where.notOperator", logicalOperator);
            }
            JsonNode conditions = queryObj.get(logicalOperator);
            if (!conditions.isArray()) {
                throw new BizException("module.save.check.where.notArray", logicalOperator);
            }
            if (conditions.isEmpty()) {
                throw new BizException("module.save.check.where.isEmpty", logicalOperator);
            }
            ValidationContext newContext = context.increaseDepth();
            for (JsonNode condition : conditions) {
                ValidationResult result = checkWhere(condition, newContext);
                if (!result.isValid()) {
                    return result;
                }
            }
            return ValidationResult.success();
        }
        for (Map.Entry<String, JsonNode> entry : fields) {
            eachField(entry, context);
        }
        return ValidationResult.success();
    }

    static void eachField(Map.Entry<String, JsonNode> entry, ValidationContext context) {
        String field = entry.getKey();
        JsonNode condition = entry.getValue();
        if (field.startsWith("$")) {
            if (!MONGO_OPERATORS.contains(field)) {
                throw new BizException("module.save.check.where.notOperator", field);
            }
        }
        if (context.getFieldWhitelist() != null &&
                !context.getFieldWhitelist().contains(field)) {
            throw new BizException("module.save.check.where.notField", field);
        }
        if (!condition.isObject()) {
            return;
        }
        ObjectNode conditionObj = (ObjectNode) condition;
        Set<Map.Entry<String, JsonNode>> operators = conditionObj.properties();
        for (Map.Entry<String, JsonNode> opEntry : operators) {
            String operator = opEntry.getKey();
            JsonNode value = opEntry.getValue();
            if (!operator.startsWith("$")) {
                throw new BizException("module.save.check.where.notOperator", operator);
            }
            if (!context.getAllowedOperators().contains(operator)) {
                throw new BizException("module.save.check.where.notAllowedOperator", operator);
            }
            checkOperator(operator, value, context);
        }
    }

    static void checkOperator(String operator, JsonNode value, ValidationContext context) {
        switch (operator) {
            case "$eq", "$ne":
                break;
            case "$gt", "$gte", "$lt", "$lte":
                gtOrGteOrLtOrLte(operator, value);
                break;
            case "$in", "$nin", "$all":
                inOrNinOrAll(operator, value);
                break;
            case "$regex":
                regex(operator, value);
                break;
            case "$options":
                options(operator, value);
                break;
            case "$exists":
                exists(operator, value);
                break;
            case "$type":
                type(operator, value);
                break;
            case "$mod":
                mod(operator, value);
                break;
            case "$size":
                size(operator, value);
                break;
            case "$elemMatch", "$not":
                elementOrNot(operator, value, context);
                break;
            case "$text":
                text(value);
                break;
            case "$where":
                where(value);
                break;
            default:
                throw new BizException("module.save.check.where.unKnownOperator", operator);
        }
    }

    protected static boolean isCustomParam(JsonNode value) {
        if (null == value || !value.isTextual()) {
            return false;
        }
        Matcher matcher = TEMPLATE_PATTERN.matcher(value.textValue());
        return matcher.matches();
    }

    protected static void gtOrGteOrLtOrLte(String operator, JsonNode value) {
        if (!isCustomParam(value) && !value.isNumber() && !value.isTextual()) {
            throw new BizException("module.save.check.where.notNumberOrDate", operator);
        }
    }

    protected static void inOrNinOrAll(String operator, JsonNode value) {
        if (!isCustomParam(value) && !value.isArray()) {
            throw new BizException("module.save.check.where.notArray", operator);
        }
    }

    protected static void regex(String operator, JsonNode value) {
        if (isCustomParam(value)) {
            return;
        }
        if (!value.isTextual()) {
            throw new BizException("module.save.check.where.notString", operator);
        }
        try {
            Pattern.compile(value.asText());
        } catch (Exception e) {
            throw new BizException("module.save.check.where.notRegex", value.asText());
        }
    }

    protected static void options(String operator, JsonNode value) {
        if (!isCustomParam(value) && !value.isTextual()) {
            throw new BizException("module.save.check.where.notString", operator);
        }
    }

    protected static void exists(String operator, JsonNode value) {
        if (!isCustomParam(value) && !value.isBoolean()) {
            throw new BizException("module.save.check.where.notBoolean", operator);
        }
    }

    protected static void type(String operator, JsonNode value) {
        if (!isCustomParam(value) && !value.isNumber() && !value.isTextual()) {
            throw new BizException("module.save.check.where.notNumberOrString", operator);
        }
    }

    protected static void mod(String operator, JsonNode value) {
        if (!isCustomParam(value) && !value.isArray() || value.size() != 2 ||
                !value.get(0).isNumber() || !value.get(1).isNumber()) {
            throw new BizException("module.save.check.where.notNumberArray", operator);
        }
    }

    protected static void size(String operator, JsonNode value) {
        if (!isCustomParam(value) && !value.isNumber()) {
            throw new BizException("module.save.check.where.notNumber", operator);
        }
    }

    protected static void elementOrNot(String operator, JsonNode value, ValidationContext context) {
        if (isCustomParam(value)) {
            return;
        }
        if (!value.isObject()) {
            throw new BizException("module.save.check.where.notObject", operator);
        }
        checkWhere(value, context.increaseDepth());
    }

    protected static void text(JsonNode value) {
        if (isCustomParam(value)) {
            return;
        }
        if (!value.isObject() || !value.has("$search")) {
            throw new BizException("module.save.check.where.notContainSearch");
        }
        if (!value.get("$search").isTextual()) {
            throw new BizException("module.save.check.where.notSearchString");
        }
    }

    protected static void where(JsonNode value) {
        if (!isCustomParam(value) && !value.isTextual()) {
            throw new BizException("module.save.check.where.notWhereString");
        }
    }

    public static class ValidationContext {
        private int currentDepth = 0;
        private int maxDepth = 5;
        private Set<String> allowedOperators = MONGO_OPERATORS;
        private Set<String> fieldWhitelist = null;

        public ValidationContext() {
            this(10, null);
        }

        public ValidationContext(int maxDepth) {
            this(maxDepth, null);
        }

        public ValidationContext(int maxDepth, Collection<String> fieldWhitelist) {
            this.maxDepth = Math.min(50, Math.max(1, maxDepth));
            this.fieldWhitelist = Optional.ofNullable(fieldWhitelist)
                    .map(Collection::stream)
                    .map(stream -> stream.collect(Collectors.toSet()))
                    .orElse(null);
        }

        public ValidationContext increaseDepth() {
            ValidationContext newContext = new ValidationContext(10, null);
            newContext.currentDepth = this.currentDepth + 1;
            newContext.maxDepth = this.maxDepth;
            newContext.allowedOperators = this.allowedOperators;
            newContext.fieldWhitelist = this.fieldWhitelist;
            return newContext;
        }

        // Getters and setters
        public int getCurrentDepth() {
            return currentDepth;
        }

        public int getMaxDepth() {
            return maxDepth;
        }

        public Set<String> getAllowedOperators() {
            return allowedOperators;
        }

        public Set<String> getFieldWhitelist() {
            return fieldWhitelist;
        }
    }

    public static class ValidationResult {
        private final boolean valid;
        private final String error;

        private ValidationResult(boolean valid, String error) {
            this.valid = valid;
            this.error = error;
        }

        public static ValidationResult success() {
            return new ValidationResult(true, null);
        }

        public static ValidationResult failure(String error) {
            return new ValidationResult(false, error);
        }

        public boolean isValid() {
            return valid;
        }

        public String getError() {
            return error;
        }
    }
}