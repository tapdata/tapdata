
package com.tapdata.tm.inspect.bean;

import java.util.List;
import java.util.Map;

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

	/** Maps source result column names to target result column names. */
	private Map<String, String> fieldMapping;

	/** Result columns used to deterministically order multi-row custom_query results. */
	private List<String> sortFields;

    private  String webScript;
}
