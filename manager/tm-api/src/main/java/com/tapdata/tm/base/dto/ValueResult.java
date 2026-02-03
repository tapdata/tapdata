package com.tapdata.tm.base.dto;

import com.tapdata.tm.base.field.CollectionField;

public enum ValueResult {
    EMPTY_CHAR(""),
    ZERO_INT(0),
    ZERO_LONG(0L),
    $("$"),
    $MATCH("$match"),
    $GROUP("$group"),
    $MAX("$max"),
    $MIN("$min")
    ;

    final Object value;

    <T> ValueResult(T res) {
        this.value = res;
    }

    public <T> T as() {
        return (T) this.value;
    }

    public String concat(CollectionField field) {
        return concat(field.field());
    }

    public String concat(String field) {
        if (value instanceof String iStr) {
            return iStr.concat(field);
        }
        return String.valueOf(value).concat(field);
    }
}
