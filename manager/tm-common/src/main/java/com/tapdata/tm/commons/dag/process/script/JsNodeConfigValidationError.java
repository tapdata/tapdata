package com.tapdata.tm.commons.dag.process.script;

import lombok.AllArgsConstructor;
import lombok.Data;

/** Field-level validation error returned when a script parameter is invalid. */
@Data
@AllArgsConstructor
public class JsNodeConfigValidationError {
    private int index;
    private String key;
    private String code;
    private String message;
}
