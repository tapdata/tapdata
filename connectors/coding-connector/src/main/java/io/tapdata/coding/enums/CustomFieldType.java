package io.tapdata.coding.enums;

import io.tapdata.coding.utils.tool.Checker;

import java.util.List;
import java.util.Map;

public enum CustomFieldType {
    DEFAULT("", "StringNormal", String.class),
    SELECT_SINGLE("SELECT_SINGLE", "StringNormal", String.class),//List<String>
    SELECT_MULTI("SELECT_MULTI", "StringNormal", List.class),
    TEXT_SINGLE_LINE("TEXT_SINGLE_LINE", "StringSmaller", String.class),
    TEXT_MULTI_LINE("TEXT_MULTI_LINE", "StringLonger", String.class),
    SELECT_MEMBER_SINGLE("SELECT_MEMBER_SINGLE", "StringNormal", Map.class),
    SELECT_MEMBER_MULTI("SELECT_MEMBER_MULTI", "StringNormal", List.class),
    TEXT_DATE("TEXT_DATE", "StringMinor", String.class),
    TEXT_DATETIME("TEXT_DATETIME", "StringMinor", String.class),
    TEXT_INTEGER("TEXT_INTEGER", "Integer", Integer.class),
    TEXT_DECIMAL("TEXT_DECIMAL", "StringMinor", Double.class),
    SELECT_ITERATION_SINGLE("SELECT_ITERATION_SINGLE", "StringNormal", Map.class),
    SELECT_ITERATION_MULTI("SELECT_ITERATION_MULTI", "StringNormal", List.class);
    private String name;
    private String type;
    private Class feature;

    CustomFieldType(String name, String type, Class feature) {
        this.name = name;
        this.feature = feature;
        this.type = type;
    }

    public static String type(String name) {
        if (Checker.isEmpty(name)) return DEFAULT.getType();
        CustomFieldType[] values = CustomFieldType.values();
        for (CustomFieldType value : values) {
            if (name.equals(value.getName())) {
                return value.getType();
            }
        }
        return DEFAULT.getType();
    }

    public static Class feature(String name) {
        if (Checker.isEmpty(name)) return DEFAULT.getFeature();
        CustomFieldType[] values = CustomFieldType.values();
        for (CustomFieldType value : values) {
            if (name.equals(value.getName())) {
                return value.getFeature();
            }
        }
        return DEFAULT.getFeature();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Class getFeature() {
        return feature;
    }

    public void setFeature(Class feature) {
        this.feature = feature;
    }
}
