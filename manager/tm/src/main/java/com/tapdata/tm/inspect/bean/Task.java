
package com.tapdata.tm.inspect.bean;

import lombok.Data;

@Data
public class Task {
    private String taskId;
    /** */
    private Long batchSize;
    /** */
    private Object compareFn;
    /** */
    private Object confirmFn;
    /** */
    private Boolean fullMatch;
    /** */
    private Object limit;
    /** */
    private String script;
    /** */
    private Boolean showAdvancedVerification;
    /** */
    private Source source;
    /** */
    private Source target;

    private  String webScript;
}
