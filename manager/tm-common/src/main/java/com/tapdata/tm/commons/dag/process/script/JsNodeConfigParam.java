package com.tapdata.tm.commons.dag.process.script;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/** A user-defined key/value entry persisted on a script node. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JsNodeConfigParam implements Serializable {
    private static final long serialVersionUID = 1L;

    private String key;
    private JsNodeConfigValueType type;
    private Object value;
    private boolean encrypted;
    private String description;
}
