package com.tapdata.tm.dql;

public enum DqlClassificationConfidenceEnum {
    EXACT,
    RULE,
    UNKNOWN_SINGLE;

    public static DqlClassificationConfidenceEnum parse(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        for (DqlClassificationConfidenceEnum confidence : values()) {
            if (confidence.name().equalsIgnoreCase(value)) {
                return confidence;
            }
        }
        return null;
    }
}
